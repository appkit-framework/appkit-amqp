<?php

namespace AppKit\Amqp;

use AppKit\Client\AbstractClient;

class AmqpClient extends AbstractClient implements AmqpClientInterface {
    private $options;

    function __construct(
        $log,
        $host     = '127.0.0.1',
        $port     = 5672,
        $user     = 'guest',
        $password = 'guest',
        $vhost    = '/'
    ) {
        parent::__construct($log -> withModule(static::class));

        $this -> options = [
            'host'     => $host,
            'port'     => $port,
            'user'     => $user,
            'password' => $password,
            'vhost'    => $vhost
        ];
    }

    public function declareExchange(
        $exchange,
        $type,
        $passive = false,
        $durable = false,
        $autoDelete = true,
        $internal = false,
        $arguments = []
    ) {
        $this -> getConnection() -> declareExchange(
            $exchange,
            $type,
            $passive,
            $durable,
            $autoDelete,
            $internal,
            $arguments
        );
        return $this;
    }

    public function deleteExchange(
        $exchange,
        $ifUnused = false
    ) {
        $this -> getConnection() -> deleteExchange(
            $exchange,
            $ifUnused
        );
        return $this;
    }

    public function bindExchange(
        $destination,
        $source,
        $routingKey = '',
        $arguments = []
    ) {
        $this -> getConnection() -> bindExchange(
            $destination,
            $source,
            $routingKey,
            $arguments
        );
        return $this;
    }

    public function unbindExchange(
        $destination,
        $source,
        $routingKey = '',
        $arguments = []
    ) {
        $this -> getConnection() -> unbindExchange(
            $destination,
            $source,
            $routingKey,
            $arguments
        );
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
        $this -> getConnection() -> declareQueue(
            $queue,
            $passive,
            $durable,
            $exclusive,
            $autoDelete,
            $arguments
        );
        return $this;
    }

    public function deleteQueue(
        $queue,
        $ifUnused = false,
        $ifEmpty = false
    ) {
        $this -> getConnection() -> deleteQueue(
            $queue,
            $ifUnused,
            $ifEmpty
        );
        return $this;
    }

    public function bindQueue(
        $queue,
        $exchange,
        $routingKey = '',
        $arguments = []
    ) {
        $this -> getConnection() -> bindQueue(
            $queue,
            $exchange,
            $routingKey,
            $arguments
        );
        return $this;
    }

    public function unbindQueue(
        $queue,
        $exchange,
        $routingKey = '',
        $arguments = []
    ) {
        $this -> getConnection() -> unbindQueue(
            $queue,
            $exchange,
            $routingKey,
            $arguments
        );
        return $this;
    }

    public function purgeQueue($queue) {
        $this -> getConnection() -> purgeQueue($queue);
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
        return $this -> getConnection() -> publish(
            $body,
            $headers,
            $exchange,
            $routingKey,
            $mandatory,
            $immediate,
            $confirm
        );
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
        return $this -> getConnection() -> consume(
            $queue,
            $callback,
            $consumerTag,
            $noLocal,
            $noAck,
            $exclusive,
            $arguments,
            $concurrency,
            $prefetchCount
        );
    }

    public function cancelConsumer($consumerTag) {
        $this -> getConnection() -> cancelConsumer($consumerTag);
        return $this;
    }

    protected function createConnection() {
        return new AmqpConnection($this -> log, $this -> options);
    }
}
