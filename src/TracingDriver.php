<?php declare(strict_types=1);
namespace Nevay\OTelInstrumentation\DoctrineDbal;

use Doctrine\DBAL\Connection\StaticServerVersionProvider;
use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Driver\API\ExceptionConverter;
use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\Driver\Exception;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\DB2Platform;
use Doctrine\DBAL\Platforms\MariaDBPlatform;
use Doctrine\DBAL\Platforms\MySQLPlatform;
use Doctrine\DBAL\Platforms\OraclePlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Platforms\SqlitePlatform;
use Doctrine\DBAL\Platforms\SQLServerPlatform;
use Doctrine\DBAL\ServerVersionProvider;
use OpenTelemetry\API\Trace\SpanKind;
use OpenTelemetry\API\Trace\StatusCode;
use OpenTelemetry\API\Trace\TracerInterface;
use SensitiveParameter;
use Throwable;
use function sprintf;

final class TracingDriver implements Driver {

    private const DB_SYSTEMS = [
        SQLServerPlatform::class => 'mssql',
        MariaDBPlatform::class => 'mariadb',
        MySQLPlatform::class => 'mysql',
        OraclePlatform::class => 'oracle',
        DB2Platform::class => 'db2',
        PostgreSQLPlatform::class => 'postgresql',
        SqlitePlatform::class => 'sqlite',
    ];

    public function __construct(
        private readonly Driver $driver,
        private readonly TracerInterface $tracer,
    ) {}

    public function connect(#[SensitiveParameter] array $params): Connection {
        $attributes = [
            'db.system' => 'other_sql',
            'db.namespace' => $params['dbname'] ?? null,
            'server.address' => $params['host'] ?? null,
            'server.port' => $params['port'] ?? null,
            'network.peer.name' => $params['host'] ?? null,
            'network.peer.port' => $params['port'] ?? null,
        ];

        if (($serverVersion = $params['serverVersion'] ?? $params['primary']['serverVersion'] ?? null) !== null) {
            $attributes['db.system'] = self::resolveDbSystem($this->driver->getDatabasePlatform(new StaticServerVersionProvider($serverVersion)));
        }

        $span = $this->tracer
            ->spanBuilder(sprintf('CONNECT %s', self::resolveSpanName($attributes)))
            ->setSpanKind(SpanKind::KIND_CLIENT)
            ->setAttributes($attributes)
            ->setAttribute('code.function', __FUNCTION__)
            ->setAttribute('code.namespace', $this->driver::class)
            ->startSpan();
        $scope = $span->activate();
        try {
            $connection = $this->driver->connect($params);

            if ($serverVersion === null) {
                $attributes['db.system'] = self::resolveDbSystem($this->driver->getDatabasePlatform($connection));
                $span->updateName(sprintf('CONNECT %s', self::resolveSpanName($attributes)));
                $span->setAttribute('db.system', $attributes['db.system']);
            }
        } catch (Throwable $e) {
            if ($e instanceof Exception) {
                $span->setAttribute('db.response.status_code', $e->getSQLState());
            }

            $span->setStatus(StatusCode::STATUS_ERROR, $e->getMessage());
            $span->recordException($e, ['exception.escaped' => true]);
            $span->setAttribute('error.type', $e::class);

            throw $e;
        } finally {
            $scope->detach();
            $span->end();
        }

        return new TracingConnection(
            $connection,
            $this->tracer,
            $attributes,
        );
    }

    public function getDatabasePlatform(ServerVersionProvider $versionProvider): AbstractPlatform {
        return $this->driver->getDatabasePlatform($versionProvider);
    }

    public function getExceptionConverter(): ExceptionConverter {
        return $this->driver->getExceptionConverter();
    }

    private function resolveSpanName(array $attributes): string {
        if (isset($attributes['server.address'])) {
            $name = $attributes['server.address'];

            if (isset($attributes['server.port'])) {
                $name .= ':';
                $name .= $attributes['server.port'];
            }

            return $name;
        }

        return $attributes['db.system'];
    }

    private static function resolveDbSystem(AbstractPlatform $platform): string {
        if ($system = self::DB_SYSTEMS[$platform::class] ?? null) {
            return $system;
        }

        foreach (self::DB_SYSTEMS as $type => $system) {
            if ($platform instanceof $type) {
                return $system;
            }
        }

        return 'other_sql';
    }
}
