# OpenTelemetry `doctrine/dbal` instrumentation

## Installation

```shell
composer require tbachert/otel-instrumentation-doctrine-dbal
```

## Usage

### Manual registration

```php
use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\DriverManager;
use Nevay\OTelInstrumentation\DoctrineDbal\TracingMiddleware;

$config = new Configuration();
$config->setMiddlewares([
    new TracingMiddleware($tracerProvider),
]);
$connection = DriverManager::getConnection($params, $config);
```

### Automatic registration

The tracing middleware is automatically injected if auto-instrumentation is enabled for the project.
