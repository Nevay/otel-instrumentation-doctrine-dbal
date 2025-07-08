<?php declare(strict_types=1);
namespace Nevay\OTelInstrumentation\DoctrineDbal;

use Closure;
use Doctrine\DBAL\Driver\Exception;
use OpenTelemetry\API\Trace\SpanInterface;
use OpenTelemetry\API\Trace\StatusCode;
use PhpMyAdmin\SqlParser\Components\JoinKeyword;
use PhpMyAdmin\SqlParser\Context;
use PhpMyAdmin\SqlParser\Parser;
use PhpMyAdmin\SqlParser\Statement;
use PhpMyAdmin\SqlParser\Statements\AlterStatement;
use PhpMyAdmin\SqlParser\Statements\CreateStatement;
use PhpMyAdmin\SqlParser\Statements\DeleteStatement;
use PhpMyAdmin\SqlParser\Statements\DropStatement;
use PhpMyAdmin\SqlParser\Statements\InsertStatement;
use PhpMyAdmin\SqlParser\Statements\RenameStatement;
use PhpMyAdmin\SqlParser\Statements\ReplaceStatement;
use PhpMyAdmin\SqlParser\Statements\SelectStatement;
use PhpMyAdmin\SqlParser\Statements\TransactionStatement;
use PhpMyAdmin\SqlParser\Statements\TruncateStatement;
use PhpMyAdmin\SqlParser\Statements\UpdateStatement;
use PhpMyAdmin\SqlParser\Statements\WithStatement;
use PhpMyAdmin\SqlParser\TokensList;
use PhpMyAdmin\SqlParser\TokenType;
use PhpMyAdmin\SqlParser\Utils\Query;
use ReflectionFunction;
use Throwable;
use function array_splice;
use function array_unique;
use function assert;
use function count;
use function implode;
use function mb_substr;
use function sprintf;
use function strlen;

/**
 * @internal
 */
final class Util {

    public static function trace(SpanInterface $span, Closure $closure, mixed ...$arguments): mixed {
        $scope = $span->activate();
        try {
            Util::reflectCodeAttributes($span, $closure);
            return $closure(...$arguments);
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
    }

    /**
     * @see https://opentelemetry.io/docs/specs/semconv/general/attributes/#source-code-attributes
     */
    public static function reflectCodeAttributes(SpanInterface $span, Closure $closure): void {
        if (!$span->isRecording()) {
            return;
        }

        $reflection = new ReflectionFunction($closure);

        if (!$reflection->isAnonymous()) {
            $span->setAttribute('code.function.name', $reflection->getClosureCalledClass()
                ? $reflection->getClosureCalledClass()->getMethod($reflection->name)->getDeclaringClass()->name . '::' . $reflection->name
                : $reflection->name
            );
        }
        if ($reflection->getFileName() !== false) {
            $span->setAttribute('code.file.path', $reflection->getFileName());
        }
        if ($reflection->getStartLine() !== false) {
            $span->setAttribute('code.line.number', $reflection->getStartLine());
        }
    }

    public static function resolveQuerySpanName(array $attributes): string {
        $name = $attributes['db.query.summary'] ?? null;
        if ($name !== null) {
            return $name;
        }

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

    public static function attributes(string $sql, bool $sanitizeQueryText = true): array {
        $mode = Context::getMode();
        Context::setMode(Context::SQL_MODE_ANSI | Context::SQL_MODE_NO_ENCLOSING_QUOTES);
        try {
            $parser = new Parser($sql);

            $operations = [];
            $collections = [];
            $summaries = [];

            $statements = $parser->statements;
            for ($i = count($statements); --$i >= 0;) {
                $s = $statements[$i];
                if ($s instanceof WithStatement) {
                    array_splice($statements, $i, 1);
                }
                if ($s instanceof TransactionStatement) {
                    array_splice($statements, $i, 1, $s->statements ?? []);
                }
            }
            foreach ($statements as $statement) {
                if ($statement instanceof TransactionStatement) {
                    $operations[] = $statement->options->build();
                    $collections[] = null;
                    continue;
                }

                $summary = [];
                $tables = self::summarize($statement, $summary);
                $flags = Query::getFlags($statement);

                $operations[] = $flags->queryType?->value;
                $collections[] = $tables[0] ?? null;
                $summaries[] = implode(' ', $summary) ?: null;
            }

            $attributes = [];

            if (count(array_unique($collections)) === 1) {
                $attributes['db.collection.name'] = $collections[0];
            }
            if (count(array_unique($operations)) === 1) {
                $attributes['db.operation.name'] = $operations[0];
            }
            if (count($statements) > 1) {
                $attributes['db.operation.name'] = isset($attributes['db.operation.name'])
                    ? sprintf('BATCH %s', $attributes['db.operation.name'])
                    : 'BATCH';
                $attributes['db.operation.batch.size'] = count($statements);
            }
            if (count(array_unique($summaries)) === 1) {
                $attributes['db.query.summary'] = $summaries[0];
            }

            $attributes['db.query.text'] = $sanitizeQueryText
                ? self::sanitize($sql, $parser->list)
                : $sql;

            return $attributes;
        } finally {
            Context::setMode($mode);
        }
    }

    private static function sanitize(string $sql, TokensList $list): string {
        $offset = null;
        $sanitized = '';
        foreach ($list->tokens as $token) {
            $offset ??= $token->position;
            if ($token->type === TokenType::Bool || $token->type === TokenType::Number || $token->type === TokenType::String) {
                $sanitized .= mb_substr($sql, $offset, $token->position - $offset, 'UTF-8');
                $sanitized .= '?';
                $offset = null;
            }
        }

        if ($sanitized === '') {
            return $sql;
        }
        if ($offset !== null) {
            $sanitized .= mb_substr($sql, $offset, null, 'UTF-8');
        }

        return $sanitized;
    }

    /**
     * @see Query::getTables()
     */
    private static function summarize(Statement $statement, array &$summary = [], int $summaryLength = 0): array {
        $expressions = [];

        if (($statement instanceof InsertStatement) || ($statement instanceof ReplaceStatement)) {
            $expressions = [$statement->into->dest];
        } elseif ($statement instanceof UpdateStatement) {
            $expressions = $statement->tables;
        } elseif (($statement instanceof SelectStatement) || ($statement instanceof DeleteStatement)) {
            $expressions = $statement->from;
        } elseif (($statement instanceof AlterStatement) || ($statement instanceof TruncateStatement)) {
            $expressions = [$statement->table];
        } elseif ($statement instanceof DropStatement) {
            if ($statement->options->has('TABLE') || $statement->options->has('VIEW')) {
                $expressions = $statement->fields;
            }
        } elseif ($statement instanceof CreateStatement) {
            if (($statement->options->has('TABLE') || $statement->options->has('VIEW')) && $statement->name) {
                $expressions = [$statement->name];
            }
        } elseif ($statement instanceof RenameStatement) {
            foreach ($statement->renames as $rename) {
                $expressions[] = $rename->old;
            }
        }
        foreach ($statement->join ?? [] as $join) {
            if (assert($join instanceof JoinKeyword) && $join->expr) {
                $expressions[] = $join->expr;
            }
        }

        $flags = Query::getFlags($statement);
        if ($flags->queryType) {
            $summaryLength += strlen($flags->queryType->value);

            if ($summaryLength <= 255) {
                $summary[] = $flags->queryType->value;
            }
        }

        $tables = [];
        foreach ($expressions as $expr) {
            if ($expr->table !== null) {
                $tables[] = $expr->expr;

                $summaryLength += strlen($expr->expr);
                if ($summaryLength <= 255) {
                    $summary[] = $expr->expr;
                }
            }
            if ($expr->subquery !== null) {
                foreach ((new Parser($expr->expr))->statements as $statement) {
                    self::summarize($statement, $summary, $summaryLength);
                }
            }
        }

        if (($statement instanceof InsertStatement || $statement instanceof ReplaceStatement) && $statement->select) {
            self::summarize($statement->select, $summary, $summaryLength);
        }

        return $tables;
    }
}
