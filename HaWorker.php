<?php


/**
 * high available work处理器
 * created by guozhucheng
 * date: 2015/5/13
 */
class HaWorker extends Zookeeper {


    /**
     * work 节点名称前缀
     */
    const  WORK_NAME_PREFIX = 'WORK_';

    /**
     * leader 节点名称
     */
    const  LEADER_NAME = 'LEADER';

    /**
     * 实例节点名称
     * @var string
     */
    protected $insNode = '/default';

    /**
     * 最大刷新时间
     * @var int
     */
    protected $maxFreTime = 60;

    /**
     * logger器
     * @var iHaWorkerLog
     */
    protected $logger;

    /**
     * 默认zk acl配置
     * @var array
     */
    protected $acl = array(
        array(
            'perms'  => Zookeeper::PERM_ALL,
            'scheme' => 'world',
            'id'     => 'anyone'
        )
    );

    /**
     * 是否是leader
     * @var bool
     */
    private $isLeader = false;

    /**
     * 节点名称
     * @var string
     */
    private $znode;


    /**
     * work 构造函数
     * @param string $host
     * 主机名称，格式为'host:port',如果是集群方式部署，则使用'host1:port1,host2:port2'
     * @param array  $confs 配置信息
     * @param int    $recv_timeout 接受超时时间
     */
    public function __construct($host = '', $confs = null, $recv_timeout = 10000) {
        parent::__construct($host, null, $recv_timeout);
        //设置配置信息
        if (isset($confs['insnode'])) {
            $this->insNode = $confs['insnode'];
        }
        if (isset($confs['maxfreshtime'])) {
            $this->maxFreTime = $confs['maxfreshtime'];
        }

        //配置Log
        if (isset($confs['log']) && $confs['log'] instanceof iHaWorkerLog) {
            $this->logger = $confs['log'];
        } else {
            $this->logger = new HaWorkerLog();
        }

        $this->register();
    }

    /**
     * 设置leader 标示
     * @param bool $flag
     */
    private function setLeader($flag) {
        $this->isLeader = $flag;
    }

    /**
     * 获取work 节点前缀
     * @return string
     */
    private function getNodePre() {
        return $this->insNode . '/' . self::WORK_NAME_PREFIX;
    }

    /**
     * work 注册节点
     */
    private function register() {
        if (!$this->exists($this->insNode)) {
            $this->create($this->insNode, null, $this->acl);
        }
        $timeStamp = microtime(true); //work 节点保存的值为当前时间戳

        /**
         * 创建work节点
         * 模式说明：
         * PERSISTENT 持久化目录znode
         * PERSISTENT_SEQUENTIAL 顺序自动编号的目录znode。这个目录节点是根据当前已存在的节点数递增。
         * EPHEMERAL 临时目录znode，一旦创建这个znode的客户端和服务器断开，这个节点就会自动删除。临时节点（EPHEMERAL）不能有子节点数据
         * EPHEMERAL_SEQUENTIAL 临时自动编号znode。
         */
        $this->znode = $this->create($this->getNodePre(), $timeStamp, $this->acl, Zookeeper::EPHEMERAL | Zookeeper::SEQUENCE);
        $this->znode = str_replace($this->insNode . '/', '', $this->znode);
        if (empty($this->znode)) {
            $this->logger->errorLog(sprintf("create %s node faield", $this->znode));

            return false;
        }
        $this->logger->infoLog(sprintf("registered as %s", $this->znode));

    }


    /**
     * 刷新操作
     */
    private function refresh() {

        $this->logger->infoLog(sprintf("%s is refresh", $this->znode));

        //获取所有的节点
        $subNodes  = $this->getChildren($this->insNode);
        $timeStamp = microtime(true);
        /**
         * 删除过期节点
         */
        foreach ($subNodes as $nodeIndex => $nodeName) {
            $nodeVal = $this->get($this->insNode . '/' . $nodeName);
            // 节点已经超时 则删除节点
            if ($timeStamp - floatval($nodeVal) >= $this->maxFreTime) {
                $this->logger->warnLog(sprintf("%s is timeout, remove it", $nodeName));
                $nodeName = $this->insNode . '/' . $nodeName;
                $this->delete($nodeName);
                unset($subNodes[$nodeIndex]);
            }
        }
        if (!in_array($this->znode, $subNodes)) {
            //如果自己已经不存在，则退出
            $logInfo = sprintf("%s not in worklist ,exits", $this->znode);
            $this->logger->warnLog($logInfo);
            exit;
        }

        //刷新自己在zk中的时间
        $this->set($this->insNode . '/' . $this->znode, $timeStamp);
        $logInfo = sprintf("refresh work  %s timestamp %s", $this->znode, $timeStamp);
        $this->logger->infoLog($logInfo);
        //如果自己是leader 并且 leader节点存在 则刷新 leader节点时间
        if ($this->isLeader() && in_array(self::LEADER_NAME, $subNodes)) {
            $this->set($this->insNode . '/' . self::LEADER_NAME, $timeStamp);
            $logInfo = sprintf("refresh leader  %s timestamp %s", $this->znode, $timeStamp);
            $this->logger->infoLog($logInfo);
        } //如果leader 不存在，则将自己设置为leader
        else if (!in_array(self::LEADER_NAME, $subNodes)) {
            $this->create($this->insNode . '/' . self::LEADER_NAME, $timeStamp, $this->acl, Zookeeper::EPHEMERAL);
            $this->setLeader(true);
            $logInfo = sprintf("set  %s as leader", $this->znode);
            $this->logger->infoLog($logInfo);
        }
    }

    /**
     * run works
     */
    public function run() {

        while (true) {
            $this->refresh();
            if ($this->isLeader()) {
                $this->doLeaderJob();
            } else {
                $this->doWorkerJob();
            }
        }
    }

    /**
     * 判断是否是leader
     * @return bool
     */
    public function isLeader() {
        return $this->isLeader;
    }

    protected function doLeaderJob() {
        echo "Leading\n";
        sleep(10);
    }

    protected function doWorkerJob() {
        echo "Working\n";
        sleep(1);
    }

}


/**
 * hawork 记日志接口
 * Interface iHaWorkerLog
 */
interface iHaWorkerLog {

    /**
     * 错误日志
     * @param array|object|string $info 错误日志信息
     * @return mixed
     */
    public function errorLog($info);

    /**
     * 信息输出日志
     * @param array|object|string $info
     * @return mixed
     */
    public function infoLog($info);

    /**
     * 警告日志
     * @param array|object|string $info
     * @return mixed
     */
    public function warnLog($info);

    public function debugLog($info);
}

/**
 * iHaWorkerLog 实现
 * Class HaWorkerLog
 */
class HaWorkerLog implements iHaWorkerLog {
    /**
     * 将日志信息转化成
     * @param mixed $infos ，需要格式化的信息
     */
    private static function formartInfo($infos) {
        $str = '';
        if (is_array($infos)) {
            $str .= '(';
            foreach ($infos as $key => $val) {
                $str .= $key . '=' . self::formartInfo($val) . '&';
            }
            $str = substr($str, 0, -1);
            $str .= ')';
        } elseif (is_object($infos)) {
            $className = get_class();
            if (method_exists($infos, '__toString')) {
                $str .= 'class_' . $className . '{' . self::formartInfo($infos->__toString()) . '}';
            } else {
                $vars    = get_object_vars($infos);
                $strVars = self::formartInfo($vars);
                $str .= 'class_' . $className . '{' . $strVars . '}';
            }
        } else {
            $arrEscapeChar = array(
//                ' '  => '%20',
//                '&'  => '%26',
//                '='  => '%3D',
//                "\r" => '%0D',
//                "\n" => '%0A'
            );
            $str .= strtr($infos, $arrEscapeChar);
        }

        return $str;
    }

    /**
     * 信息输出日志
     * @param array|object|string $info
     * @return mixed
     */
    public function infoLog($info) {
        printf("INFO:%s\n", self::formartInfo($info));
    }

    /**
     * 错误日志
     * @param array|object|string $info 错误日志信息
     * @return mixed
     */
    public function errorLog($info) {
        printf("ERROR:%s\n", self::formartInfo($info));
    }

    /**
     * 警告日志
     * @param array|object|string $info
     * @return mixed
     */
    public function warnLog($info) {
        printf("WARN:%s\n", self::formartInfo($info));
    }

    public function debugLog($info) {
        printf("DEBUG:%s\n", self::formartInfo($info));
    }
}
