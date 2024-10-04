<?php declare(strict_types=1);
namespace Nevay\OTelInstrumentation\DoctrineDbal;

use Composer\InstalledVersions;
use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Driver\Middleware;
use OpenTelemetry\API\Trace\TracerInterface;
use OpenTelemetry\API\Trace\TracerProviderInterface;

final class TracingMiddleware implements Middleware {

    private readonly TracerProviderInterface $tracerProvider;
    private readonly TracerInterface $tracer;

    public function __construct(TracerProviderInterface $tracerProvider) {
        $this->tracerProvider = $tracerProvider;
        $this->tracer = $tracerProvider->getTracer(
            'com.tobiasbachert.instrumentation.doctrine-dbal',
            InstalledVersions::getPrettyVersion('tbachert/otel-instrumentation-doctrine-dbal'),
            'https://opentelemetry.io/schemas/1.27.0',
        );
    }

    public function equals(mixed $other): bool {
        return $other instanceof self && $this->tracerProvider === $other->tracerProvider;
    }

    public function wrap(Driver $driver): Driver {
        return new TracingDriver($driver, $this->tracer);
    }
}
