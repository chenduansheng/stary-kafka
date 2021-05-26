<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4 foldmethod=marker: */
// +---------------------------------------------------------------------------
// | SWAN [ $_SWANBR_SLOGAN_$ ]
// +---------------------------------------------------------------------------
// | Copyright $_SWANBR_COPYRIGHT_$
// +---------------------------------------------------------------------------
// | Version  $_SWANBR_VERSION_$
// +---------------------------------------------------------------------------
// | Licensed ( $_SWANBR_LICENSED_URL_$ )
// +---------------------------------------------------------------------------
// | $_SWANBR_WEB_DOMAIN_$
// +---------------------------------------------------------------------------

namespace Kafka;

/**
+------------------------------------------------------------------------------
* Kafka protocol since Kafka v0.8
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class KafkaConsumer
{
    use \Psr\Log\LoggerAwareTrait;
    use \Kafka\LoggerTrait;

    // {{{ consts
    // }}}
    // {{{ members

    private $defultConfig = [
        'MetadataRefreshIntervalMs'=>10000,
        'MetadataBrokerList'=>'192.168.59.100:9093, 192.168.59.100:9094, 92.168.59.100:9095',
        'GroupId'=>'test4',
        'BrokerVersion'=>'1.0.0',
        'Topics'=>['test']
    ];

    private $isRunning = false;

    // }}}
    // {{{ functions
    // {{{ public function __construct()

    /**
     * __construct
     *
     * @access public
     * @param $hostList
     * @param null $timeout
     */
    public function __construct($config)
    {
        $config = array_merge($this->defultConfig, $config);

        $configObj = \Kafka\ConsumerConfig::getInstance();
        $configObj->setMetadataRefreshIntervalMs($config['MetadataRefreshIntervalMs']);
        $configObj->setMetadataBrokerList($config['MetadataBrokerList']);
        $configObj->setGroupId($config['GroupId']);
        $configObj->setBrokerVersion($config['BrokerVersion']);
        $configObj->setTopics($config['Topics']);

    }

    // }}}
    // {{{ public function start()

    /**
     * start consumer
     *
     * @access public
     * @return void
     */
    public function start(\Closure $consumer = null, $isBlock = true)
    {
        if ($this->isRunning) {
            $this->error('Has start consumer');
            return;
        }
        $process = new \Kafka\Consumer\Process($consumer);
        if ($this->logger) {
            $process->setLogger($this->logger);
        }
        $process->start();
        $this->isRunning = true;
        if ($isBlock) {
            \Amp\run();
        }
    }

    // }}}
    // }}}
}
