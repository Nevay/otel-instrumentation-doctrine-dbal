<?php declare(strict_types=1);
namespace Nevay\OTelInstrumentation\DoctrineDbal;

use Doctrine\DBAL\DriverManager;
use OpenTelemetry\API\Configuration\ConfigProperties;
use OpenTelemetry\API\Instrumentation\AutoInstrumentation\Context;
use OpenTelemetry\API\Instrumentation\AutoInstrumentation\HookManagerInterface;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(DoctrineInstrumentation::class)]
final class InstrumentationTest extends TestCase {

    public function testInstrumentationHooksSocketServerConstructor(): void {
        $hookManager = $this->createMock(HookManagerInterface::class);
        $configProperties = $this->createMock(ConfigProperties::class);

        $hookManager->expects($this->once())->method('hook')->with(DriverManager::class, 'getConnection');

        $instrumentation = new DoctrineInstrumentation();
        $instrumentation->register($hookManager, $configProperties, new Context());
    }

    public function testInstrumentationCanBeDisabled(): void {
        $hookManager = $this->createMock(HookManagerInterface::class);
        $configProperties = $this->createMock(ConfigProperties::class);

        $hookManager->expects($this->never())->method('hook');
        $configProperties->method('get')->with(DoctrineConfiguration::class)->willReturn(new DoctrineConfiguration(enabled: false));

        $instrumentation = new DoctrineInstrumentation();
        $instrumentation->register($hookManager, $configProperties, new Context());
    }
}
