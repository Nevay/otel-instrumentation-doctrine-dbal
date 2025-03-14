<?php declare(strict_types=1);
namespace Nevay\OTelInstrumentation\DoctrineDbal;

use OpenTelemetry\API\Instrumentation\AutoInstrumentation\InstrumentationConfiguration;

final class DoctrineConfiguration implements InstrumentationConfiguration {

    public function __construct(
        public readonly bool $captureParameters = false,
    ) {}
}
