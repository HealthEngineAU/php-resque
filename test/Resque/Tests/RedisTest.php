<?php

namespace Resque\Tests;

use Redis as PhpRedis;
use Resque\Exceptions\RedisException;
use Resque\Redis;
use Resque\Resque;

/**
 * Redis tests.
 *
 * @package		Resque/Tests
 * @author		Chris Boulton <chris@bigcommerce.com>
 * @license		http://www.opensource.org/licenses/mit-license.php
 * @covers \Resque\Redis
 */
class RedisTest extends ResqueTestCase
{
    public function testRedisExceptionsAreSurfaced()
    {
		$mockRedis = $this->createPartialMock(PhpRedis::class, ['connect', 'ping']);
		$mockRedis
			->expects($this->any())
			->method('ping')
            ->willThrowException(new \RedisException('failure'));

        Resque::setBackend(function ($database) use ($mockRedis) {
            return new Redis('localhost:6379', $database, $mockRedis);
        });

		$this->expectException(RedisException::class);

        Resque::redis()->ping();
    }

    /**
     * These DNS strings are considered valid.
     *
     * @return array
     */
    public static function validDsnStringProvider()
    {
        return array(
            // Input , Expected output
            array('', array(
                'localhost',
                Redis::DEFAULT_PORT,
                false,
                false, false,
                array(),
            )),
            array('localhost', array(
                'localhost',
                Redis::DEFAULT_PORT,
                false,
                false, false,
                array(),
            )),
            array('localhost:1234', array(
                'localhost',
                1234,
                false,
                false, false,
                array(),
            )),
            array('localhost:1234/2', array(
                'localhost',
                1234,
                2,
                false, false,
                array(),
            )),
            array('redis://foobar', array(
                'foobar',
                Redis::DEFAULT_PORT,
                false,
                false, false,
                array(),
            )),
            array('redis://foobar/', array(
                'foobar',
                Redis::DEFAULT_PORT,
                false,
                false, false,
                array(),
            )),
            array('redis://foobar:1234', array(
                'foobar',
                1234,
                false,
                false, false,
                array(),
            )),
            array('redis://foobar:1234/15', array(
                'foobar',
                1234,
                15,
                false, false,
                array(),
            )),
            array('redis://foobar:1234/0', array(
                'foobar',
                1234,
                0,
                false, false,
                array(),
            )),
            array('redis://user@foobar:1234', array(
                'foobar',
                1234,
                false,
                'user', false,
                array(),
            )),
            array('redis://user@foobar:1234/15', array(
                'foobar',
                1234,
                15,
                'user', false,
                array(),
            )),
            array('redis://user:pass@foobar:1234', array(
                'foobar',
                1234,
                false,
                'user', 'pass',
                array(),
            )),
            array('redis://user:pass@foobar:1234?x=y&a=b', array(
                'foobar',
                1234,
                false,
                'user', 'pass',
                array('x' => 'y', 'a' => 'b'),
            )),
            array('redis://:pass@foobar:1234?x=y&a=b', array(
                'foobar',
                1234,
                false,
                false, 'pass',
                array('x' => 'y', 'a' => 'b'),
            )),
            array('redis://user@foobar:1234?x=y&a=b', array(
                'foobar',
                1234,
                false,
                'user', false,
                array('x' => 'y', 'a' => 'b'),
            )),
            array('redis://foobar:1234?x=y&a=b', array(
                'foobar',
                1234,
                false,
                false, false,
                array('x' => 'y', 'a' => 'b'),
            )),
            array('redis://user@foobar:1234/12?x=y&a=b', array(
                'foobar',
                1234,
                12,
                'user', false,
                array('x' => 'y', 'a' => 'b'),
            )),
            array('tcp://user@foobar:1234/12?x=y&a=b', array(
                'foobar',
                1234,
                12,
                'user', false,
                array('x' => 'y', 'a' => 'b'),
            )),
        );
    }

    /**
     * These DSN values should throw exceptions
     * @return array
     */
    public static function bogusDsnStringProvider()
    {
        return array(
            array('http://foo.bar/'),
            array('user:@foobar:1234?x=y&a=b'),
            array('foobar:1234?x=y&a=b'),
        );
    }

    /**
     * @dataProvider validDsnStringProvider
     */
    public function testParsingValidDsnString($dsn, $expected)
    {
        $result = Redis::parseDsn($dsn);
        $this->assertEquals($expected, $result);
    }

    /**
     * @dataProvider bogusDsnStringProvider
     */
    public function testParsingBogusDsnStringThrowsException($dsn)
    {
		$this->expectException(\InvalidArgumentException::class);

        Redis::parseDsn($dsn);
    }
}
