<?php declare(strict_types=1);
namespace Nevay\OTelInstrumentation\DoctrineDbal;

use Nevay\OTelInstrumentation\DoctrineDbal;
use Nevay\OTelSDK\Configuration\Config;
use Nevay\OTelSDK\Configuration\Env\ArrayEnvSource;
use Nevay\OTelSDK\Configuration\Env\EnvSourceReader;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(DoctrineConfiguration::class)]
#[CoversClass(DoctrineDbal\Config\InstrumentationConfigurationDoctrineDbalConfig::class)]
#[CoversClass(DoctrineDbal\ConfigEnv\InstrumentationConfigurationDoctrineDbalConfig::class)]
final class ConfigTest extends TestCase {

    public function testKitchenSink(): void {
        $result = Config::loadFile(__DIR__ . '/snippets/kitchen-sink.yaml');

        $config = $result->configProperties->get(DoctrineConfiguration::class);

        $this->assertInstanceOf(DoctrineConfiguration::class, $config);
        $this->assertTrue($config->enabled);
        $this->assertTrue($config->captureParameters);
    }

    public function testDisabled(): void {
        $result = Config::loadFile(__DIR__ . '/snippets/disabled.yaml');

        $config = $result->configProperties->get(DoctrineConfiguration::class);

        $this->assertInstanceOf(DoctrineConfiguration::class, $config);
        $this->assertFalse($config->enabled);
    }

    public function testEnv(): void {
        $result = Config::loadFromEnv(new EnvSourceReader([new ArrayEnvSource([])]));

        $config = $result->configProperties->get(DoctrineConfiguration::class);

        $this->assertInstanceOf(DoctrineConfiguration::class, $config);
        $this->assertTrue($config->enabled);
    }

    public function testEnvDisabled(): void {
        $result = Config::loadFromEnv(new EnvSourceReader([new ArrayEnvSource(['OTEL_PHP_DISABLED_INSTRUMENTATIONS' => 'doctrine-dbal'])]));

        $config = $result->configProperties->get(DoctrineConfiguration::class);

        $this->assertInstanceOf(DoctrineConfiguration::class, $config);
        $this->assertFalse($config->enabled);
    }

    public function testEnvDisabledAll(): void {
        $result = Config::loadFromEnv(new EnvSourceReader([new ArrayEnvSource(['OTEL_PHP_DISABLED_INSTRUMENTATIONS' => 'all'])]));

        $config = $result->configProperties->get(DoctrineConfiguration::class);

        $this->assertInstanceOf(DoctrineConfiguration::class, $config);
        $this->assertFalse($config->enabled);
    }
}
