<?php

namespace Resque\Tests;

use Monolog\Handler\TestHandler;
use Monolog\Logger;
use Resque\Worker\ResqueWorker;
use Resque\Event;
use Resque\JobHandler;
use Resque\Resque;
use Resque\Exceptions\DoNotCreateException;
use Resque\Exceptions\DoNotPerformException;
use Test_Job;

/**
 * Event tests.
 *
 * @package		Resque/Tests
 * @author		Chris Boulton <chris@bigcommerce.com>
 * @license		http://www.opensource.org/licenses/mit-license.php
 * @covers \Resque\Event
 */
class EventTest extends ResqueTestCase
{
    private $callbacksHit = array();

    protected function setUp(): void
    {
        Test_Job::$called = false;

        $this->loggerHandler = new TestHandler();
        $this->logger = new Logger('testing', [$this->loggerHandler]);

        // Register a worker to test with
        $this->worker = new ResqueWorker('jobs');
        $this->worker->setLogger($this->logger);
        $this->worker->registerWorker();
    }

    protected function tearDown(): void
    {
        Event::clearListeners();
        $this->callbacksHit = array();
    }

    public function getEventTestJob()
    {
        $payload = array(
            'class' => 'Test_Job',
            'args' => array(
                array('somevar'),
            ),
        );
        $job = new JobHandler('jobs', $payload);
        $job->worker = $this->worker;
        return $job;
    }

    public static function eventCallbackProvider()
    {
        return array(
            array('beforePerform', 'beforePerformEventCallback'),
            array('afterPerform', 'afterPerformEventCallback'),
            array('afterFork', 'afterForkEventCallback'),
        );
    }

    /**
     * @dataProvider eventCallbackProvider
     */
    public function testEventCallbacksFire($event, $callback)
    {
        Event::listen($event, array($this, $callback));

        $job = $this->getEventTestJob();
        $this->worker->perform($job);
        $this->worker->work(0);

        $this->assertContains($callback, $this->callbacksHit, $event . ' callback (' . $callback . ') was not called');
        $this->assertTrue($this->loggerHandler->hasDebug([
            'context' => [],
            'message' => 'Registered signals',
        ]));
        $this->assertTrue($this->loggerHandler->hasInfo([
            'context' => ['queue' => 'jobs'],
            'message' => 'Checking {queue} for jobs',
        ]));
        $this->assertTrue($this->loggerHandler->hasNotice([
            'context' => ['job' => $job],
            'message' => '{job} has finished',
        ]));
    }

    public function testBeforeForkEventCallbackFires()
    {
        $event = 'beforeFork';
        $callback = 'beforeForkEventCallback';

        Event::listen($event, array($this, $callback));
        Resque::enqueue('jobs', 'Test_Job', array(
            'somevar'
        ));

        $this->worker->work(0);
        $this->assertContains($callback, $this->callbacksHit, $event . ' callback (' . $callback . ') was not called');
    }

    public function testBeforeEnqueueEventCallbackFires()
    {
        $event = 'beforeEnqueue';
        $callback = 'beforeEnqueueEventCallback';

        Event::listen($event, array($this, $callback));
        Resque::enqueue('jobs', 'Test_Job', array(
            'somevar'
        ));
        $this->assertContains($callback, $this->callbacksHit, $event . ' callback (' . $callback . ') was not called');
    }

    public function testBeforePerformEventCanStopWork()
    {
        $callback = 'beforePerformEventDontPerformCallback';
        Event::listen('beforePerform', array($this, $callback));

        $job = $this->getEventTestJob();

        $this->assertFalse($job->perform());
        $this->assertContains($callback, $this->callbacksHit, $callback . ' callback was not called');
        $this->assertFalse(Test_Job::$called, 'Job was still performed though DoNotPerformException was thrown');
    }

    public function testBeforeEnqueueEventStopsJobCreation()
    {
        $callback = 'beforeEnqueueEventDontCreateCallback';
        Event::listen('beforeEnqueue', array($this, $callback));
        Event::listen('afterEnqueue', array($this, 'afterEnqueueEventCallback'));

        $result = Resque::enqueue('test_job', 'TestClass');
        $this->assertContains($callback, $this->callbacksHit, $callback . ' callback was not called');
        $this->assertNotContains('afterEnqueueEventCallback', $this->callbacksHit, 'afterEnqueue was still called, even though it should not have been');
        $this->assertFalse($result);
    }

    public function testAfterEnqueueEventCallbackFires()
    {
        $callback = 'afterEnqueueEventCallback';
        $event    = 'afterEnqueue';

        Event::listen($event, array($this, $callback));
        Resque::enqueue('jobs', 'Test_Job', array(
            'somevar'
        ));
        $this->assertContains($callback, $this->callbacksHit, $event . ' callback (' . $callback . ') was not called');
    }

    public function testStopListeningRemovesListener()
    {
        $callback = 'beforePerformEventCallback';
        $event    = 'beforePerform';

        Event::listen($event, array($this, $callback));
        Event::stopListening($event, array($this, $callback));

        $job = $this->getEventTestJob();
        $this->worker->perform($job);
        $this->worker->work(0);

        $this->assertNotContains(
            $callback,
            $this->callbacksHit,
            $event . ' callback (' . $callback . ') was called though Event::stopListening was called'
        );
    }

    public function beforePerformEventDontPerformCallback($instance)
    {
        $this->callbacksHit[] = __FUNCTION__;
        throw new DoNotPerformException();
    }

    public function beforeEnqueueEventDontCreateCallback($queue, $class, $args, $id, $track = false)
    {
        $this->callbacksHit[] = __FUNCTION__;
        throw new DoNotCreateException();
    }

    public function assertValidEventCallback($function, $job)
    {
        $this->callbacksHit[] = $function;
        if (!$job instanceof JobHandler) {
            $this->fail('Callback job argument is not an instance of JobHandler');
        }
        $args = $job->getArguments();
        $this->assertEquals($args[0], 'somevar');
    }

    public function afterEnqueueEventCallback($args, $class, $id, $queue)
    {
        $this->callbacksHit[] = __FUNCTION__;
        $this->assertEquals('Test_Job', $class);
        $this->assertEquals(array(
            'somevar',
        ), $args);
        $this->assertEquals('jobs', $queue);
    }

    public function beforeEnqueueEventCallback($args, $class, $id, $queue)
    {
        $this->callbacksHit[] = __FUNCTION__;
    }

    public function beforePerformEventCallback($job)
    {
        $this->assertValidEventCallback(__FUNCTION__, $job);
    }

    public function afterPerformEventCallback($job)
    {
        $this->assertValidEventCallback(__FUNCTION__, $job);
    }

    public function beforeForkEventCallback($job)
    {
        $this->assertValidEventCallback(__FUNCTION__, $job);
    }

    public function afterForkEventCallback($job)
    {
        $this->assertValidEventCallback(__FUNCTION__, $job);
    }
}
