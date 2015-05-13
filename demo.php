<?php
require_once 'HaWorker.php';

class DemoHaWork extends HaWorker {
    public function __construct() {
        parent::__construct('127.0.0.1:2181', array(
            'insnode'      => '/demo',
            'maxfreshtime' => 100,
        ));
    }

    function doLeaderJob() {
        echo "Demo Leading\n";
        sleep(10);
    }

    function doWorkerJob() {
        echo "Demo Working\n";
        sleep(1);
    }
}

$worker = new DemoHaWork();
$worker->run();
