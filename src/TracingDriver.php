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

final class TracingDriver implements Driver {

    private const DB_SYSTEMS = [
        SQLServerPlatform::class => 'microsoft.sql_server',
        MariaDBPlatform::class => 'mariadb',
        MySQLPlatform::class => 'mysql',
        OraclePlatform::class => 'oracle.db',
        DB2Platform::class => 'ibm.db2',
        PostgreSQLPlatform::class => 'postgresql',
        SqlitePlatform::class => 'sqlite',
    ];

    public function __construct(
        private readonly Driver $driver,
        private readonly TracerInterface $tracer,
        private readonly DoctrineConfiguration $config,
    ) {}

    public function connect(#[SensitiveParameter] array $params): Connection {
        $attributes = [
            'db.system.name' => 'other_sql',
            'db.namespace' => $params['dbname'] ?? null,
            'server.address' => $params['host'] ?? null,
            'server.port' => $params['port'] ?? null,
        ];

        if (($serverVersion = $params['serverVersion'] ?? $params['primary']['serverVersion'] ?? null) !== null) {
            $attributes['db.system.name'] = self::resolveDbSystem($this->driver->getDatabasePlatform(new StaticServerVersionProvider($serverVersion)));
        }

        $span = $this->tracer
            ->spanBuilder(Util::resolveConnectionSpanName($attributes, 'CONNECT'))
            ->setSpanKind(SpanKind::KIND_CLIENT)
            ->setAttributes($attributes)
            ->startSpan();
        $scope = $span->activate();
        try {
            Util::reflectCodeAttributes($span, $this->driver->connect(...));
            $connection = $this->driver->connect($params);

            if ($serverVersion === null) {
                $span->setAttribute('db.system.name', $attributes['db.system.name'] = self::resolveDbSystem($this->driver->getDatabasePlatform($connection)));
            }
        } catch (Throwable $e) {
            if ($e instanceof Exception && $e->getSQLState() !== null) {
                $span->setAttribute('db.response.status_code', $e->getSQLState());
                $span->setAttribute('error.type', $e->getSQLState());
            } else {
                $span->setAttribute('error.type', $e::class);
            }

            $span->setStatus(StatusCode::STATUS_ERROR, $e->getMessage());
            $span->recordException($e);

            throw $e;
        } finally {
            $scope->detach();
            $span->end();
        }

        return new TracingConnection(
            $connection,
            $this->tracer,
            $this->config,
            $attributes,
        );
    }

    public function getDatabasePlatform(ServerVersionProvider $versionProvider): AbstractPlatform {
        return $this->driver->getDatabasePlatform($versionProvider);
    }

    public function getExceptionConverter(): ExceptionConverter {
        return $this->driver->getExceptionConverter();
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
