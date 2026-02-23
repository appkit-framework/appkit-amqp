<?php

namespace AppKit\Amqp;

use AppKit\Amqp\Internal\AmqpInterface;

use AppKit\Client\AbstractClientConnection;
use function AppKit\Async\async;
use function AppKit\Async\await;
use function AppKit\Async\delay;

use Throwable;
use Bunny\Client;
use Bunny\Protocol\MethodBasicNackFrame;
use React\Promise\Deferred;

class AmqpConnection extends AbstractClientConnection implements AmqpInterface {
    private $options;

    private $log;
    private $bunny;
    private $channels = [];
    private $consumers = [];
    private $confirmDeferreds = [];
    private $returnedMessages = [];

    /****************************************
     * CONSTRUCTOR
     ****************************************/
    
    function __construct($log, $options) {
        $this -> log = $log -> withModule(static::class);
        $this -> options = $options;
    }

    /****************************************
     * AMQP METHODS
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
        $this -> ensureConnected();
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
    }

    public function deleteExchange(
        $exchange,
        $ifUnused = false
    ) {
        $this -> ensureConnected();
        $this -> callChannel('default', 'exchangeDelete', [
            $exchange,
            $ifUnused
        ]);
    }

    public function bindExchange(
        $destination,
        $source,
        $routingKey = '',
        $arguments = []
    ) {
        $this -> ensureConnected();
        $this -> callChannel('default', 'exchangeBind', [
            $destination,
            $source,
            $routingKey,
            false,
            $arguments
        ]);
    }

    public function unbindExchange(
        $destination,
        $source,
        $routingKey = '',
        $arguments = []
    ) {
        $this -> ensureConnected();
        $this -> callChannel('default', 'exchangeUnbind', [
            $destination,
            $source,
            $routingKey,
            false,
            $arguments
        ]);
    }

    public function declareQueue(
        $queue,
        $passive = false,
        $durable = false,
        $exclusive = false,
        $autoDelete = true,
        $arguments = []
    ) {
        $this -> ensureConnected();
        $this -> callChannel('default', 'queueDeclare', [
            $queue,
            $passive,
            $durable,
            $exclusive,
            $autoDelete,
            false,
            $arguments
        ]);
    }

    public function deleteQueue(
        $queue,
        $ifUnused = false,
        $ifEmpty = false
    ) {
        $this -> ensureConnected();
        $this -> callChannel('default', 'queueDelete', [
            $queue,
            $ifUnused,
            $ifEmpty
        ]);
    }

    public function bindQueue(
        $queue,
        $exchange,
        $routingKey = '',
        $arguments = []
    ) {
        $this -> ensureConnected();
        $this -> callChannel('default', 'queueBind', [
            $exchange,
            $queue,
            $routingKey,
            false,
            $arguments
        ]);
    }

    public function unbindQueue(
        $queue,
        $exchange,
        $routingKey = '',
        $arguments = []
    ) {
        $this -> ensureConnected();
        $this -> callChannel('default', 'queueUnbind', [
            $exchange,
            $queue,
            $routingKey,
            $arguments
        ]);
    }

    public function purgeQueue($queue) {
        $this -> ensureConnected();
        $this -> callChannel('default', 'queuePurge', [ $queue ]);
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
        $this -> ensureConnected();

        if(($mandatory || $immediate) && !$confirm)
            throw new AmqpConnectionException('Mandatory or immediate requires confirm to be true');

        $channelName = 'publish';
        if($confirm)
            $channelName .= '_confirm';

        if(!isset($this -> channels[$channelName])) {
            $this -> callChannel($channelName, 'addReturnListener', [
                function($message, $frame) {
                    $this -> onMessageReturn($message, $frame);
                }
            ]);
            $this -> log -> debug(
                'Configured channel return listener',
                [ 'channelName' => $channelName ]
            );

            if($confirm) {
                try {
                    $this -> callChannel($channelName, 'confirmSelect', [
                        function($frame) {
                            return $this -> onConfirmFrame($frame);
                        }
                    ]);
                    $this -> log -> debug(
                        'Enabled channel confirm mode',
                        [ 'channelName' => $channelName ]
                    );
                } catch(Throwable $e) {
                    $error = 'Failed to enable channel confirm mode';
                    $this -> log -> error(
                        $error,
                        [ 'channelName' => $channelName ],
                        $e
                    );
                    throw new AmqpConnectionException(
                        $error,
                        previous: $e
                    );
                }
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
            } catch(Throwable $e) {
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
            throw new AmqpConnectionException("Consumer tag $consumerTag already in use");
        $channelName = "consume_$consumerTag";

        $prefetchCount ??= $concurrency;
        try {
            $this -> callChannel($channelName, 'qos', [
                0,
                $prefetchCount
            ]);
            $this -> log -> debug(
                'Set channel prefetch count',
                [ 'channelName' => $channelName, 'prefetchCount' => $prefetchCount ]
            );
        } catch(Throwable $e) {
            $error = 'Failed to set channel prefetch count';
            $this -> log -> error(
                $error,
                [ 'channelName' => $channelName, 'prefetchCount' => $prefetchCount ],
                $e
            );
            throw new AmqpConnectionException(
                $error,
                previous: $e
            );
        }

        $this -> callChannel($channelName, 'consume', [
            async(function($message) use($consumerTag) {
                $this -> handleMessage($message, $consumerTag);
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
        $this -> log -> debug(
            'Created consumer',
            [
                'consumerTag' => $consumerTag,
                'queue' => $queue,
                'noLocal' => $noLocal,
                'noAck' => $noAck,
                'exclusive' => $exclusive,
                'arguments' => $arguments,
                'concurrency' => $concurrency,
                'prefetchCount' => $prefetchCount
            ]
        );

        return $consumerTag;
    }

    public function cancelConsumer($consumerTag) {
        $this -> ensureConnected();
        $this -> cancelConsumerInt($consumerTag);
    }

    /****************************************
     * CONNECTION METHODS
     ****************************************/

    protected function doConnect() {
        $this -> bunny = new Client($this -> options);
        $this -> bunny -> once('close', function() {
            $this -> setClosed();
            $this -> cleanup();
        });
        $this -> bunny -> connect();
    }

    protected function doDisconnect() {
        // Cancel all consumers
        $consumerCount = count($this -> consumers);
        if($consumerCount > 0) {
            $this -> log -> warning(
                'Disconnect with active consumers',
                [ 'consumerCount' => $consumerCount ]
            );

            foreach($this -> consumers as $consumerTag => $_) {
                try {
                    $this -> cancelConsumerInt($consumerTag);
                    $this -> log -> info(
                        'Canceled consumer',
                        [ 'consumerTag' => $consumerTag ]);
                } catch(Throwable $e) {
                    $this -> log -> error(
                        'Failed to cancel consumer',
                        [ 'consumerTag' => $consumerTag ],
                        $e
                    );
                }
            }
        }

        // Disconnect
        $this -> bunny -> disconnect();
    }

    /****************************************
     * CONNECTION INTERNALS
     ****************************************/

    private function cleanup() {
        // Reject all pending consumer cancelations
        foreach($this -> consumers as $consumerTag => $consumer) {
            if($consumer['pendingMessages'])
                $this -> log -> warning(
                    'Consumer aborted during message processing',
                    [
                        'consumerTag' => $consumerTag,
                        'pendingMessages' => $consumer['pendingMessages']
                    ]
                );

            if($consumer['cancelDeferred'])
                $consumer['cancelDeferred'] -> reject(
                    new AmqpConnectionException('Connection lost while still processing messages')
                );
        }

        // Reject all delivery confirmations
        $unconfirmedMessages = count($this -> confirmDeferreds);
        if($unconfirmedMessages > 0) {
            $this -> log -> warning(
                'Connection closed while awaiting delivery confirmations',
                [ 'unconfirmedMessages' => $unconfirmedMessages ]
            );

            foreach($this -> confirmDeferreds as $deferred)
                $deferred -> reject(
                    new AmqpConnectionException('Connection lost before delivery confirmation')
                );
        }
    }

    /****************************************
     * CHANNEL INTERNALS
     ****************************************/

    private function callChannel($channelName, $method, $args = []) {
        $channel = $this -> getChannel($channelName);

        try {
            return $channel -> $method(...$args);
        } catch(Throwable $e) {
            throw new AmqpConnectionException(
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
            $channel = $this -> bunny -> channel();
        } catch(Throwable $e) {
            $error = 'Failed to open new channel';
            $this -> log -> error(
                $error,
                [ 'channelName' => $channelName ],
                $e
            );
            throw new AmqpConnectionException(
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
        $this -> log -> debug(
            'Opened new channel',
            [ 'channelName' => $channelName, 'channelId' => $channelId ]
        );

        return $channel;
    }

    private function closeChannel($channelName) {
        if(!isset($this -> channels[$channelName]))
            return;

        $channelId = $this -> channels[$channelName]['channel'] -> getChannelId();
        $this -> channels[$channelName]['isClosing'] = true;
        $this -> channels[$channelName]['channel'] -> close();
        delay(0); // TODO: Same case as callChannel()

        unset($this -> channels[$channelName]);

        $this -> log -> debug(
            'Closed channel',
            [ 'channelName' => $channelName, 'channelId' => $channelId ]
        );
    }

    private function onChannelClose($channelName) {
        if(! $this -> isConnected() || $this -> channels[$channelName]['isClosing'])
            return;

        $channelId = $this -> channels[$channelName]['channel'] -> getChannelId();
        $this -> log -> warning(
            'Channel was unexpectedly closed',
            [ 'channelName' => $channelName, 'channelId' => $channelId ]
        );

        unset($this -> channels[$channelName]);
    }

    /****************************************
     * CONSUMING INTERNALS
     ****************************************/

    private function cancelConsumerInt($consumerTag) {
        if(!isset($this -> consumers[$consumerTag]))
            throw new AmqpConnectionException("Consumer $consumerTag does not exist");

        $channelName = "consume_$consumerTag";

        try {
            $this -> callChannel($channelName, 'cancel', [ $consumerTag ]);
            $this -> log -> debug(
                'Consumer canceled on the server',
                [ 'consumerTag' => $consumerTag ]
            );
        } catch(Throwable $e) {
            $this -> log -> error(
                'Failed to cancel consumer on the server',
                [ 'consumerTag' => $consumerTag ],
                $e
            );
            throw $e;
        }

        $pendingMessages = $this -> consumers[$consumerTag]['pendingMessages'];
        if($pendingMessages > 0) {
            $cancelDeferred = new Deferred();
            $this -> consumers[$consumerTag]['cancelDeferred'] = $cancelDeferred;
            $this -> log -> debug(
                'Waiting to cleanup consumer',
                [ 'consumerTag' => $consumerTag, 'pendingMessages' => $pendingMessages ]
            );
            try {
                await($cancelDeferred -> promise());
                $this -> log -> debug(
                    'Ready to cleanup consumer',
                    [ 'consumerTag' => $consumerTag ]
                );
            } catch(Throwable $e) {
                $this -> log -> warning(
                    'Forcing cleanup consumer',
                    [ 'consumerTag' => $consumerTag, 'pendingMessages' => $pendingMessages ],
                    $e
                );
            }
        }

        try {
            $this -> closeChannel($channelName);
            $this -> log -> debug(
                'Closed consumer channel',
                [ 'consumerTag' => $consumerTag, 'channelName' => $channelName ]
            );
        } catch(Throwable $e) {
            $this -> log -> error(
                'Failed to close consumer channel',
                [ 'consumerTag' => $consumerTag, 'channelName' => $channelName ],
                $e
            );
        }

        unset($this -> consumers[$consumerTag]);
        $this -> log -> debug(
            'Cleaned up consumer',
            [ 'consumerTag' => $consumerTag ]
        );
    }

    private function handleMessage($message, $consumerTag) {
        $this -> log -> setContext('amqpMsgId', $message -> headers['message-id'] ?? null);

        $this -> consumers[$consumerTag]['pendingMessages']++;

        try {
            $this -> consumers[$consumerTag]['callback']($message -> content, $message -> headers);

            $action = 'ack';
            $reason = null;
        } catch(AmqpNackReject $e) {
            $action = 'reject';
            $reason = $e -> getMessage();
        } catch(AmqpNackRequeue $e) {
            $action = 'requeue';
            $reason = $e -> getMessage();
        } catch(Throwable $e) {
            $action = 'requeue';
            $reason = 'Uncaught exception in message handler';

            $this -> log -> error(
                $reason,
                [ 'consumerTag' => $consumerTag ],
                $e
            );
        }

        if($this -> consumers[$consumerTag]['noAck']) {
            if($action != 'ack')
                $this -> log -> error(
                    'Cannot ack message consumed with noAck flag',
                    [ 'action' => $action, 'reason' => $reason, 'consumerTag' => $consumerTag ],
                    $e
                );
        } else {
            try {
                if($action == 'ack')
                    $this -> callChannel("consume_$consumerTag", 'ack', [$message]);
                else if($action == 'reject')
                    $this -> callChannel("consume_$consumerTag", 'reject', [$message, false]);
                else
                    $this -> callChannel("consume_$consumerTag", 'reject', [$message, true]);
            } catch(Throwable $e) {
                $this -> log -> error(
                    'Failed to ack message',
                    [ 'action' => $action, 'reason' => $reason, 'consumerTag' => $consumerTag ],
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
                'Received negative acknowledgment',
                [ 'multiple' => $frame -> multiple, 'deliveryTag' => $frame -> deliveryTag ]
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
                new AmqpConnectionException('Negative acknowledgment received from the server')
            );
    }

    private function onMessageReturn($message, $frame) {
        $messageId = $message -> headers['message-id'];

        $this -> log -> debug(
            'Message was returned',
            [ 'messageId' => $messageId, 'replyText' => $frame -> replyText ]
        );

        if(array_key_exists($messageId, $this -> returnedMessages))
            $this -> returnedMessages[$messageId] = new AmqpReturnException($frame -> replyText);
        else
            $this -> log -> warning(
                'Unhandled return of message',
                [ 'messageId' => $messageId ]
            );
    }

    private function genMessageId() {
        // UUID v4
        $data = random_bytes(16);
        $data[6] = chr(ord($data[6]) & 0x0f | 0x40);
        $data[8] = chr(ord($data[8]) & 0x3f | 0x80);
        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
    }
}
