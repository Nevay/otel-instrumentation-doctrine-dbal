# OpenTelemetry [doctrine/dbal] instrumentation

## Installation

```shell
composer require tbachert/otel-instrumentation-doctrine-dbal
```

## Usage

### Automatic instrumentation

This instrumentation is enabled by default.

#### Disable via file-based configuration

```yaml
instrumentations/development:
  php:
    doctrine_dbal: false
```

#### Disable via env-based configuration

```shell
OTEL_PHP_DISABLED_INSTRUMENTATIONS=doctrine-dbal
```

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

[doctrine/dbal]: https://github.com/doctrine/dbal
