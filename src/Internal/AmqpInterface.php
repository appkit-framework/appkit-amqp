<?php

namespace AppKit\Amqp\Internal;

interface AmqpInterface {
    public function declareExchange(
        $exchange,
        $type,
        $passive = false,
        $durable = false,
        $autoDelete = true,
        $internal = false,
        $arguments = []
    );

    public function deleteExchange(
        $exchange,
        $ifUnused = false
    );

    public function bindExchange(
        $destination,
        $source,
        $routingKey = '',
        $arguments = []
    );

    public function unbindExchange(
        $destination,
        $source,
        $routingKey = '',
        $arguments = []
    );

    public function declareQueue(
        $queue,
        $passive = false,
        $durable = false,
        $exclusive = false,
        $autoDelete = true,
        $arguments = []
    );

    public function deleteQueue(
        $queue,
        $ifUnused = false,
        $ifEmpty = false
    );

    public function bindQueue(
        $queue,
        $exchange,
        $routingKey = '',
        $arguments = []
    );

    public function unbindQueue(
        $queue,
        $exchange,
        $routingKey = '',
        $arguments = []
    );

    public function purgeQueue($queue);

    public function publish(
        $body,
        $headers = [],
        $exchange = '',
        $routingKey = '',
        $mandatory = false,
        $immediate = false,
        $confirm = false
    );

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
    );

    public function cancelConsumer($consumerTag);
}
