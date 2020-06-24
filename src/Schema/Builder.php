<?php namespace Adrianheras\Lumencassandra\Schema;

use Closure;
use Adrianheras\Lumencassandra\Connection;

class Builder extends \Illuminate\Database\Schema\Builder
{
    /**
     * @inheritdoc
     */
    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
        $this->grammar = $connection->getSchemaGrammar();
    }


  
    /**
     * @inheritdoc
     */
    protected function createBlueprint($table, Closure $callback = null)
    {
        return new Blueprint($this->connection, $table);
    }
}
