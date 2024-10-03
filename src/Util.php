<?php declare(strict_types=1);
namespace Nevay\OTelInstrumentation\DoctrineDbal;

use Closure;
use Doctrine\DBAL\Driver\Exception;
use OpenTelemetry\API\Trace\SpanInterface;
use OpenTelemetry\API\Trace\StatusCode;
use PhpMyAdmin\SqlParser\Context;
use PhpMyAdmin\SqlParser\Parser;
use PhpMyAdmin\SqlParser\Token;
use PhpMyAdmin\SqlParser\TokensList;
use PhpMyAdmin\SqlParser\TokenType;
use PhpMyAdmin\SqlParser\Utils\Query;
use Throwable;
use function array_intersect_assoc;
use function count;
use function sprintf;
use function strlen;
use function substr_replace;

/**
 * @internal
 */
final class Util {

    public static function trace(SpanInterface $span, Closure $closure, mixed ...$arguments): mixed {
        $scope = $span->activate();

        try {
            return $closure(...$arguments);
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
    }

    public static function resolveQuerySpanName(array $attributes): string {
        $name = $attributes['db.operation.name'] ?? 'SQL';
        if (isset($attributes['db.collection.name'])) {
            $name .= ' ';
            $name .= $attributes['db.collection.name'];
        }

        return $name;
    }

    public static function resolveConnectionSpanName(array $attributes, string $prefix): string {
        if (isset($attributes['server.address'])) {
            $name = $attributes['server.address'];

            if (isset($attributes['server.port'])) {
                $name .= ':';
                $name .= $attributes['server.port'];
            }

            return sprintf('%s %s', $prefix, $name);
        }

        return $prefix;
    }

    public static function prefixOperationName(array $attributes, string $prefix): array {
        $attributes['db.operation.name'] = isset($attributes['db.operation.name'])
            ? sprintf('%s %s', $prefix, $attributes['db.operation.name'])
            : $prefix;

        return $attributes;
    }

    public static function attributes(string $sql): array {
        $mode = Context::getMode();
        Context::setMode(Context::SQL_MODE_ANSI | Context::SQL_MODE_NO_ENCLOSING_QUOTES);
        try {
            $parser = new Parser($sql);

            $attributes = [];
            foreach ($parser->statements as $i => $statement) {
                $tables = Query::getTables($statement);
                $flags = Query::getFlags($statement);

                $attributes[$i]['db.collection.name'] = $tables[0] ?? null;
                $attributes[$i]['db.operation.name'] = $flags->queryType->value;
            }

            $attributes = array_intersect_assoc(...$attributes);

            if (count($parser->statements) > 1) {
                $attributes = self::prefixOperationName($attributes, 'BATCH');
                $attributes['db.operation.batch.size'] = count($parser->statements);
            }

            $attributes['db.query.text'] = self::sanitize($sql, $parser->list);

            return $attributes;
        } finally {
            Context::setMode($mode);
        }
    }

    private static function sanitize(string $sql, TokensList $list): string {
        for ($i = $list->count, $prev = new Token('', TokenType::Delimiter); $token = $list->tokens[--$i] ?? null; $prev = $token) {
            match ($token->type) {
                TokenType::Bool,
                TokenType::Number,
                TokenType::String,
                    => $sql = substr_replace($sql, '?', $token->position, ($prev->position ?? strlen($sql)) - $token->position),
                default,
                    => null,
            };
        }

        return $sql;
    }
}
