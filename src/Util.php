<?php declare(strict_types=1);
namespace Nevay\OTelInstrumentation\DoctrineDbal;

use Closure;
use Doctrine\DBAL\Driver\Exception;
use OpenTelemetry\API\Trace\SpanInterface;
use OpenTelemetry\API\Trace\StatusCode;
use PhpMyAdmin\SqlParser\Context;
use PhpMyAdmin\SqlParser\Parser;
use PhpMyAdmin\SqlParser\Statement;
use PhpMyAdmin\SqlParser\Statements\TransactionStatement;
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
        $name = $prefix;
        if (isset($attributes['server.address'])) {
            $name .= ' ';
            $name .= $attributes['server.address'];

            if (isset($attributes['server.port'])) {
                $name .= ':';
                $name .= $attributes['server.port'];
            }
        }

        return $name;
    }

    public static function prefixOperationName(array $attributes, string $prefix): array {
        $attributes['db.operation.name'] = isset($attributes['db.operation.name'])
            ? sprintf('%s %s', $prefix, $attributes['db.operation.name'])
            : $prefix;

        return $attributes;
    }

    public static function attributes(string $sql, bool $includeQueryText = true): array {
        $mode = Context::getMode();
        Context::setMode(Context::SQL_MODE_ANSI | Context::SQL_MODE_NO_ENCLOSING_QUOTES);
        try {
            $parser = new Parser($sql);

            $attributes = [];
            self::statementAttributes($parser->statements, $attributes);

            if ($attributes) {
                $attributes = array_intersect_assoc(...$attributes);
            }

            if (count($parser->statements) > 1) {
                $attributes = self::prefixOperationName($attributes, 'BATCH');
                $attributes['db.operation.batch.size'] = count($parser->statements);
            }
            if ($includeQueryText) {
                $attributes['db.query.text'] = self::sanitize($sql, $parser->list);
            }

            return $attributes;
        } finally {
            Context::setMode($mode);
        }
    }

    /**
     * @param list<Statement> $statements
     */
    private static function statementAttributes(array $statements, array &$attributes, int &$i = -1): void {
        foreach ($statements as $statement) {
            if ($statement instanceof TransactionStatement && $statement->statements) {
                self::statementAttributes($statement->statements, $attributes, $i);
                continue;
            }

            $tables = Query::getTables($statement);
            $flags = Query::getFlags($statement);

            $i++;
            $attributes[$i]['db.collection.name'] = $tables[0] ?? null;
            $attributes[$i]['db.operation.name'] = $flags->queryType?->value;

            if ($statement instanceof TransactionStatement && $statement->type === TransactionStatement::TYPE_BEGIN) {
                $attributes[$i]['db.operation.name'] = 'START TRANSACTION';
            }
            if ($statement instanceof TransactionStatement && $statement->type === TransactionStatement::TYPE_END) {
                $attributes[$i]['db.operation.name'] = $statement->options->build();
            }
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
