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
class KafkaProducer
{
    use \Psr\Log\LoggerAwareTrait;
    use \Kafka\LoggerTrait;

    private $defaultConfig = [
        'MetadataRefreshIntervalMs'=>10000,
        'BrokerVersion'=>'1.0.0',
        'RequiredAck'=>1,
        'IsAsyn'=>false,
        'ProduceInterval'=>500,
        'MetadataBrokerList'=>'192.168.59.100:9093, 192.168.59.100:9094, 92.168.59.100:9095',
    ];


    private $process = null;


    /**
     * __construct
     *
     * @access public
     * @param $hostList
     * @param null $timeout
     */
    public function __construct($config=[])
    {

        $config = array_merge($this->defaultConfig, $config);

        $configObj = \Kafka\ProducerConfig::getInstance();
        $configObj->setMetadataRefreshIntervalMs($config['MetadataRefreshIntervalMs']);
        $configObj->setBrokerVersion($config['BrokerVersion']);
        $configObj->setRequiredAck($config['RequiredAck']);
        $configObj->setIsAsyn($config['IsAsyn']);
        $configObj->setProduceInterval($config['ProduceInterval']);
        $configObj->setMetadataBrokerList($config['MetadataBrokerList']);

        $this->process = new \Kafka\Producer\SyncProcess();
    }

    // }}}
    // {{{ public function send()


    /**
     * start producer
     *
     * @access public
     * @data is data is boolean that is async process, thus it is sync process
     * @return void
     */
    public function send($data = true)
    {
        if ($this->logger) {
            $this->process->setLogger($this->logger);
        }
        if (is_bool($data)) {
            $this->process->start();
            if ($data) {
                \Amp\run();
            }
        } else {
            return $this->process->send($data);
        }
    }

    // }}}
    // {{{ public function syncMeta()

    /**
     * syncMeta producer
     *
     * @access public
     * @return void
     */
    public function syncMeta()
    {
        return $this->process->syncMeta();
    }

    // }}}
    // {{{ public function success()

    /**
     * producer success
     *
     * @access public
     * @return void
     */
    public function success(\Closure $success = null)
    {
        $this->process->setSuccess($success);
    }

    // }}}
    // {{{ public function error()

    /**
     * producer error
     *
     * @access public
     * @return void
     */
    public function error(\Closure $error = null)
    {
        $this->process->setError($error);
    }

    // }}}
    // }}}
}