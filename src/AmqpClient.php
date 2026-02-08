<?php

namespace AppKit\Amqp;

use AppKit\StartStop\StartStopInterface;
use AppKit\Health\HealthIndicatorInterface;
use AppKit\Health\HealthCheckResult;
use AppKit\Async\Task;
use AppKit\Async\CanceledException;
use function AppKit\Async\async;
use function AppKit\Async\await;
use function AppKit\Async\delay;

use Throwable;
use Bunny\Client;
use Bunny\Protocol\MethodBasicNackFrame;
use React\Promise\Deferred;
use Evenement\EventEmitterInterface;
use Evenement\EventEmitterTrait;

class AmqpClient implements StartStopInterface, HealthIndicatorInterface, EventEmitterInterface {
    use EventEmitterTrait;

    private $log;
    private $clientConfig;
    private $isConnected = false;
    private $isStopping = false;
    private $connectTask;
    private $channels;
    private $consumers;
    private $client;
    private $confirmDeferreds = [];
    private $returnedMessages = [];

    /****************************************
     * CONSTRUCTOR
     ****************************************/
    
    function __construct(
        $log,
        $host     = '127.0.0.1',
        $port     = 5672,
        $user     = 'guest',
        $password = 'guest',
        $vhost    = '/'
    ) {
        $this -> log = $log -> withModule(static::class);
        $this -> clientConfig = [
            'host'     => $host,
            'port'     => $port,
            'user'     => $user,
            'password' => $password,
            'vhost'    => $vhost
        ];
    }

    /****************************************
     * INTERFACE METHODS
     ****************************************/
    
    public function start() {
        $this -> connect();
    }
    
    public function stop() {
        $this -> isStopping = true;

        if($this -> connectTask -> getStatus() == Task::RUNNING) {
            $this -> log -> debug('Connect task running during stop, canceling...');
            $this -> connectTask -> cancel() -> join();
        }

        if($this -> isConnected) {
            // Cancel all consumers
            foreach($this -> consumers as $tag => $_) {
                $this -> log -> warning("Consumer $tag is still active at shutdown");
                try {
                    $this -> cancelConsumer($tag);
                    $this -> log -> info("Canceled consumer $tag");
                } catch(Throwable $e) {
                    $this -> log -> error("Failed to cancel consumer $tag", $e);
                }
            }

            // Disconnect
            try {
                $this -> client -> disconnect();
                $this -> log -> info('Disconnected from AMQP server');
            } catch(Throwable $e) {
                $error = 'Failed to disconnect from AMQP server';
                $this -> log -> error($error, $e);
                throw new AmqpClientException(
                    $error,
                    previous: $e
                );
            }
        }
    }

    public function checkHealth() {
        return new HealthCheckResult($this -> isConnected);
    }

    /****************************************
     * PUBLIC METHODS
     ****************************************/

    public function declareExchange(
        $exchange,
        $type,
        $passive = false,
        $durable = false,
        $autoDelete = true,
        $internal = false,
        $arguments = []
    ) {
        $this -> callChannel('default', 'exchangeDeclare', [
            $exchange,
            $type,
            $passive,
            $durable,
            $autoDelete,
            $internal,
            false,
            $arguments
        ]);
        return $this;
    }

    public function deleteExchange(
        $exchange,
        $ifUnused = false
    ) {
        $this -> callChannel('default', 'exchangeDelete', [
            $exchange,
            $ifUnused
        ]);
        return $this;
    }

    public function bindExchange(
        $destination,
        $source,
        $routingKey = '',
        $arguments = []
    ) {
        $this -> callChannel('default', 'exchangeBind', [
            $destination,
            $source,
            $routingKey,
            false,
            $arguments
        ]);
        return $this;
    }

    public function unbindExchange(
        $destination,
        $source,
        $routingKey = '',
        $arguments = []
    ) {
        $this -> callChannel('default', 'exchangeUnbind', [
            $destination,
            $source,
            $routingKey,
            false,
            $arguments
        ]);
        return $this;
    }

    public function declareQueue(
        $queue,
        $passive = false,
        $durable = false,
        $exclusive = false,
        $autoDelete = true,
        $arguments = []
    ) {
        $this -> callChannel('default', 'queueDeclare', [
            $queue,
            $passive,
            $durable,
            $exclusive,
            $autoDelete,
            false,
            $arguments
        ]);
        return $this;
    }

    public function deleteQueue(
        $queue,
        $ifUnused = false,
        $ifEmpty = false
    ) {
        $this -> callChannel('default', 'queueDelete', [
            $queue,
            $ifUnused,
            $ifEmpty
        ]);
        return $this;
    }

    public function bindQueue(
        $queue,
        $exchange,
        $routingKey = '',
        $arguments = []
    ) {
        $this -> callChannel('default', 'queueBind', [
            $exchange,
            $queue,
            $routingKey,
            false,
            $arguments
        ]);
        return $this;
    }

    public function unbindQueue(
        $queue,
        $exchange,
        $routingKey = '',
        $arguments = []
    ) {
        $this -> callChannel('default', 'queueUnbind', [
            $exchange,
            $queue,
            $routingKey,
            $arguments
        ]);
        return $this;
    }

    public function purgeQueue($queue) {
        $this -> callChannel('default', 'queuePurge', [ $queue ]);
        return $this;
    }

    public function publish(
        $body,
        $headers = [],
        $exchange = '',
        $routingKey = '',
        $mandatory = false,
        $immediate = false,
        $confirm = false
    ) {
        if(($mandatory || $immediate) && !$confirm)
            throw new AmqpClientException('Mandatory or immediate requires confirm to be true');

        $this -> ensureConnected();

        $channelName = 'publish';
        if($confirm)
            $channelName .= '_confirm';

        if(!isset($this -> channels[$channelName])) {
            $this -> callChannel($channelName, 'addReturnListener', [
                function($message, $frame) {
                    return $this -> onMessageReturn($message, $frame);
                }
            ]);
            $this -> log -> debug("Configured return listener on channel $channelName");

            if($confirm) {
                try {
                    $this -> callChannel($channelName, 'confirmSelect', [
                        function($frame) {
                            return $this -> onConfirmFrame($frame);
                        }
                    ]);
                    $this -> log -> debug("Enabled confirm mode on channel $channelName");
                } catch(Throwable $e) {
                    $error = 'Failed to enable confirm mode';
                    $this -> log -> error("$error on channel $channelName");
                    throw new AmqpClientException(
                        $error,
                        previous: $e
                    );
                }

                $this -> confirmDeferreds = [];
            }
        }

        $messageId = $this -> genMessageId();
        $headers['message-id'] = $messageId;

        // Returns delivery tag only in confirm mode
        $deliveryTag = $this -> callChannel($channelName, 'publish', [
            $body,
            $headers,
            $exchange,
            $routingKey,
            $mandatory,
            $immediate
        ]);

        if($confirm) {
            $confirmDeferred = new Deferred();
            $this -> confirmDeferreds[$deliveryTag] = $confirmDeferred;

            $this -> returnedMessages[$messageId] = null;

            $exception = null;

            try {
                await($confirmDeferred -> promise());
            } catch(AmqpClientException $e) {
                $exception = $e;
            }
            unset($this -> confirmDeferreds[$deliveryTag]);

            if($this -> returnedMessages[$messageId]) {
                $exception = $this -> returnedMessages[$messageId];
                unset($this -> returnedMessages[$messageId]);
            }

            if($exception)
                throw $exception;
        }

        return $messageId;
    }

    public function consume(
        $queue,
        $callback,
        $consumerTag = null,
        $noLocal = false,
        $noAck = false,
        $exclusive = false,
        $arguments = [],
        $concurrency = 1,
        $prefetchCount = null
    ) {
        $this -> ensureConnected();

        $consumerTag ??= bin2hex(random_bytes(8));
        if(isset($this -> consumers[$consumerTag]))
            throw new AmqpClientException("Consumer tag $consumerTag already in use");
        $channelName = "consume_$consumerTag";

        $prefetchCount ??= $concurrency;
        try {
            $this -> callChannel($channelName, 'qos', [
                0,
                $prefetchCount
            ]);
            $this -> log -> debug("Set prefetch count to $prefetchCount on channel $channelName");
        } catch(Throwable $e) {
            $error = 'Failed to set prefetch count';
            $this -> log -> error("$error on channel $channelName", $e);
            throw new AmqpClientException(
                $error,
                previous: $e
            );
        }

        $this -> callChannel($channelName, 'consume', [
            async(function($message) use($consumerTag) {
                return $this -> handleMessage($message, $consumerTag);
            }),
            $queue,
            $consumerTag,
            $noLocal,
            $noAck,
            $exclusive,
            false,
            $arguments,
            $concurrency
        ]);

        $this -> consumers[$consumerTag] = [
            'callback' => $callback,
            'noAck' => $noAck,
            'pendingMessages' => 0,
            'cancelDeferred' => null
        ];
        $this -> log -> debug("Started consumer $consumerTag on queue \"$queue\"");

        return $consumerTag;
    }

    public function cancelConsumer($consumerTag) {
        if(!isset($this -> consumers[$consumerTag]))
            throw new AmqpClientException("Consumer $consumerTag does not exist");

        $channelName = "consume_$consumerTag";

        try {
            $this -> callChannel($channelName, 'cancel', [ $consumerTag ]);
            $this -> log -> debug("Consumer $consumerTag canceled on the server");
        } catch(Throwable $e) {
            $this -> log -> error("Failed to cancel consumer $consumerTag on the server", $e);
            throw $e;
        }

        $pendingMessages = $this -> consumers[$consumerTag]['pendingMessages'];
        if($pendingMessages > 0) {
            $cancelDeferred = new Deferred();
            $this -> consumers[$consumerTag]['cancelDeferred'] = $cancelDeferred;
            $this -> log -> debug(
                "Waiting for $pendingMessages messages before cleaning up consumer $consumerTag"
            );
            try {
                await($cancelDeferred -> promise());
                $this -> log -> debug("Ready to cleanup consumer $consumerTag");
            } catch(Throwable $e) {
                $this -> log -> warning("Forcing cleanup consumer $consumerTag", $e);
            }
        }

        unset($this -> consumers[$consumerTag]);
        try {
            $this -> closeChannel($channelName);
        } catch(Throwable $e) {
            $this -> log -> error("Failed to close channel $channelName", $e);
        }
        $this -> log -> debug("Cleaned up consumer $consumerTag");

        return $this;
    }

    /****************************************
     * CONNECTION INTERNALS
     ****************************************/

    private function connect() {
        $this -> log -> debug('Starting connect task...');

        $this -> connectTask = new Task(function() {
            return $this -> connectRoutine();
        });

        try {
            $this -> connectTask -> run() -> await();
            $this -> log -> debug('Connect task completed');
        } catch(CanceledException $e) {
            $this -> log -> info('Connect task canceled');
        }
    }

    private function connectRoutine() {
        $this -> channels = [];
        $this -> consumers = [];

        $connectDelay = null;

        while(true) {
            try {
                $this -> log -> debug('Trying to connect to AMQP server...');

                $this -> client = new Client($this -> clientConfig);
                $this -> client -> once('close', async(function() {
                   $this -> onConnectionClose();
                }));
                $this -> client -> connect();

                $this -> log -> info('Connected to AMQP server');

                break;
            } catch(Throwable $e) {
                if(! $connectDelay)
                    $connectDelay = 1;
                else if($connectDelay == 1)
                    $connectDelay = 5;
                else if($connectDelay == 5)
                    $connectDelay = 10;

                $this -> log -> error(
                    "Failed to connect to AMQP server, retrying in $connectDelay seconds",
                    $e
                );
                delay($connectDelay);
            }
        }

        $this -> isConnected = true;
        $this -> emit('connect');
    }

    private function onConnectionClose() {
        $this -> isConnected = false;

        // Reject all pending consumer cancelations
        foreach($this -> consumers as $tag => $consumer) {
            if($consumer['pendingMessages'])
                $this -> log -> warning(
                    'Connection lost while processing ' .
                    $consumer['pendingMessages'] .
                    " messages by consumer $tag"
                );

            if($consumer['cancelDeferred'])
                $consumer['cancelDeferred'] -> reject(
                    new AmqpClientException('Connection lost while still processing messages')
                );
        }

        // Reject all delivery confirmations
        $confirmDeferredCount = count($this -> confirmDeferreds);
        if($confirmDeferredCount > 0)
            $this -> log -> warning(
                "Connection lost while awaiting delivery confirmation for $confirmDeferredCount messages"
            );
        foreach($this -> confirmDeferreds as $deferred)
            $deferred -> reject(
                new AmqpClientException('Connection lost before delivery confirmation')
            );

        if($this -> isStopping)
            return;

        $this -> log -> warning('AMQP connection lost, reconnecting...');
        $this -> connect();
    }

    private function ensureConnected() {
        if(! $this -> isConnected)
            throw new AmqpClientException('Client is not connected');
    }

    /****************************************
     * CHANNEL INTERNALS
     ****************************************/

    private function callChannel($channelName, $method, $args = []) {
        $this -> ensureConnected();

        $channel = $this -> getChannel($channelName);

        try {
            return call_user_func_array([$channel, $method], $args);
        } catch(Throwable $e) {
            throw new AmqpClientException(
                $e -> getMessage(),
                previous: $e
            );
        } finally {
            /* TODO:
             * Rabbit first resolves the promise that the channel method is awaiting.
             * Then the channel method returns and further user code is executed.
             * Only then does it emit 'close'.
             * When calling two methods one after another, where the first one causes the channel to close,
             * the second will execute on the closed channel, causing the connection to be closed.
             * delay(0) returns control back to the point where 'close' is emitted.
             */
            delay(0);
        }
    }

    private function getChannel($channelName) {
        if(isset($this -> channels[$channelName]))
            return $this -> channels[$channelName]['channel'];

        try {
            $channel = $this -> client -> channel();
        } catch(Throwable $e) {
            $error = 'Failed to open channel';
            $this -> log -> error("$error $channelName", $e);
            throw new AmqpClientException(
                $error,
                previous: $e
            );
        }

        $channel -> once('close', function() use($channelName) {
            $this -> onChannelClose($channelName);
        });
        $this -> channels[$channelName] = [
            'channel' => $channel,
            'isClosing' => false
        ];

        $channelId = $channel -> getChannelId();
        $this -> log -> debug("Opened channel ${channelName}[${channelId}]");

        return $channel;
    }

    private function closeChannel($channelName) {
        if(!isset($this -> channels[$channelName])) {
            $this -> log -> warning("Trying to close non-existent channel $channelName");
            return;
        }

        $channelId = $this -> channels[$channelName]['channel'] -> getChannelId();
        $this -> channels[$channelName]['isClosing'] = true;
        $this -> channels[$channelName]['channel'] -> close();
        delay(0); // TODO: Same case as callChannel()

        unset($this -> channels[$channelName]);

        $this -> log -> debug("Closed channel ${channelName}[${channelId}]");
    }

    private function onChannelClose($channelName) {
        if($this -> isStopping || $this -> channels[$channelName]['isClosing'])
            return;

        $channelId = $this -> channels[$channelName]['channel'] -> getChannelId();
        $this -> log -> warning("Channel ${channelName}[${channelId}] was unexpectedly closed");

        unset($this -> channels[$channelName]);
    }

    /****************************************
     * CONSUMING INTERNALS
     ****************************************/
    
    private function handleMessage($message, $consumerTag) {
        $this -> consumers[$consumerTag]['pendingMessages']++;

        $messageIdStr = $message -> headers['message-id'] ?? 'without id';

        $nack = null;
        try {
            $this -> consumers[$consumerTag]['callback']($message -> content, $message -> headers);
        } catch(AmqpNackReject | AmqpNackRequeue $e) {
            $nack = $e;
        } catch(Throwable $e) {
            $this -> log -> error(
                "Uncaught exception while handling message $messageIdStr by consumer $consumerTag",
                $e
            );
            $nack = new AmqpNackRequeue('Exception in consumer callback', previous: $e);
        }

        if($this -> consumers[$consumerTag]['noAck']) {
            if($nack)
                $this -> log -> error(
                    "Cannot NACK message $messageIdStr, because noAck is set for consumer $consumerTag",
                    $e
                );
        } else {
            try {
                if($nack instanceof AmqpNackReject) {
                    $logVerb = 'reject (' . $nack -> getMessage() . ')';
                    $this -> callChannel("consume_$consumerTag", 'reject', [$message, false]);
                } else if($nack instanceof AmqpNackRequeue) {
                    $logVerb = 'requeue (' . $nack -> getMessage() . ')';
                    $this -> callChannel("consume_$consumerTag", 'reject', [$message, true]);
                } else {
                    $logVerb = 'ack';
                    $this -> callChannel("consume_$consumerTag", 'ack', [$message]);
                }
            } catch(Throwable $e) {
                $this -> log -> error(
                    "Failed to $logVerb message $messageIdStr consumed by $consumerTag",
                    $e
                );
            }
        }

        $this -> consumers[$consumerTag]['pendingMessages']--;

        if(
            $this -> consumers[$consumerTag]['pendingMessages'] == 0 &&
            $this -> consumers[$consumerTag]['cancelDeferred']
        ) {
            $this -> consumers[$consumerTag]['cancelDeferred'] -> resolve(null);
        }
    }

    /****************************************
     * PUBLISHING INTERNALS
     ****************************************/

    private function onConfirmFrame($frame) {
        $ack = ! $frame instanceof MethodBasicNackFrame;

        if(! $ack)
            $this -> log -> warning(
                'Received NACK frame for ' .
                $frame -> multiple ? 'multiple messages up to' : 'message with' .
                ' delivery tag ' .
                $frame -> deliveryTag
            );

        if($frame -> multiple) {
            foreach($this -> confirmDeferreds as $dtag => $_) {
                if($dtag > $frame -> deliveryTag)
                    break;

                $this -> fulfillConfirmDeferred($dtag, $ack);
            }
        } else {
            $this -> fulfillConfirmDeferred($frame -> deliveryTag, $ack);
        }
    }

    private function fulfillConfirmDeferred($deliveryTag, $ack) {
        if($ack)
            $this -> confirmDeferreds[$deliveryTag] -> resolve(null);
        else
            $this -> confirmDeferreds[$deliveryTag] -> reject(
                new AmqpClientException('Negative acknowledgment received from the server')
            );
    }

    private function onMessageReturn($message, $frame) {
        $messageId = $message -> headers['message-id'];

        $this -> log -> debug(
            "Message $messageId was returned with reply text: " .
            $frame -> replyText
        );

        if(array_key_exists($messageId, $this -> returnedMessages))
            $this -> returnedMessages[$messageId] = new AmqpReturnException($frame -> replyText);
        else
            $this -> log -> warning("Unhandled return of message $messageId");
    }

    private function genMessageId() {
        // UUID v4
        $data = random_bytes(16);
        $data[6] = chr(ord($data[6]) & 0x0f | 0x40);
        $data[8] = chr(ord($data[8]) & 0x3f | 0x80);
        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
    }
}
