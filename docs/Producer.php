<?php
//require '../../vendor/autoload.php';

/*
require '../src/Config.php';
require '../src/ProducerConfig.php';
require '../src/Producer.php';
require '../src/SingletonTrait.php';*/


date_default_timezone_set('PRC');

$config = \Kafka\ProducerConfig::getInstance();
$config->setMetadataRefreshIntervalMs(10000);
$config->setMetadataBrokerList('192.168.59.100:9093');
$config->setBrokerVersion('1.0.0');
$config->setRequiredAck(1);
$config->setIsAsyn(false);
$config->setProduceInterval(500);
$producer = new \Kafka\Producer();


for($i = 0; $i < 2; $i++) {
    $result = $producer->send([
        [
            'topic' => 'test',
            'value' => 'test1....message.',
            'key' => '',
        ],
    ]);
    var_dump($result);
}