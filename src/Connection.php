<?php

namespace Cubettech\Lacassa;

use Cassandra\DefaultSession;
use Cubettech\Lacassa\Helper\Helper;
use Cubettech\Lacassa\Query\Builder;
use Exception;
use Illuminate\Database\Connection as BaseConnection;
use Illuminate\Database\ConnectionResolverInterface as ConnectionResolverInterface;
use Illuminate\Support\Collection;
use Laravel\Lumen\Application;

class Connection extends BaseConnection implements ConnectionResolverInterface
{
    /**
     * The Cassandra connection handler.
     *
     * @var \Cassandra\DefaultSession
     */
    protected $connection;

    /** Create a new database connection instance. */
    public function __construct(Application $app)
    {
        $config = $this->validateConfig($app);

        // Create the connection
        $this->db = $config['keyspace'];
        $this->connection = $this->createConnection($config);
        $this->useDefaultPostProcessor();
    }

    /**
     * Dynamically pass methods to the connection.
     *
     * @param string $method
     * @param array $parameters
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        return call_user_func_array([$this->connection, $method], $parameters);
    }

    /** Begin a fluent query against a database collection. */
    public function collection(string $collection) : Builder
    {
        $query = new Query\Builder($this);

        return $query->from($collection);
    }

    /** Begin a fluent query against a database collection. */
    public function table($table, $as = null) : Builder
    {
        return $this->collection($table);
    }

    /**
     * @inheritdoc
     */
    public function getSchemaBuilder()
    {
        return new Schema\Builder($this);
    }

    /**
     * [getSchemaGrammar returns the connection grammer]
     * @return [Schema\Grammar] [description]
     */
    public function getSchemaGrammar()
    {
        return new Schema\Grammar;
    }

    /** return Cassandra object. */
    public function getCassandraConnection() : DefaultSession
    {
        return $this->connection;
    }

    /**
     * @inheritdoc
     */
    public function disconnect()
    {
        unset($this->connection);
    }

    /**
     * @inheritdoc
     */
    public function getElapsedTime($start)
    {
        return parent::getElapsedTime($start);
    }

    /**
     * @inheritdoc
     */
    public function getDriverName()
    {
        return 'Cassandra';
    }

    /**
     * Execute an CQL statement and return the boolean result.
     *
     * @param string $query
     * @param array $bindings
     * @return bool
     */
    public function statement($query, $bindings = [])
    {
        foreach ($bindings as $binding) {
            if (is_bool($binding)) {
                $value = $binding ? 'true' : 'false';
            } elseif (is_array($binding)) {
                $value = $this->elem2UDT($binding);
            } else {
                $value = Helper::isAvoidingQuotes($binding)  ? $binding : "'" . $binding . "'";
            }

            $query = preg_replace('/\?/', $value, $query, 1);
        }
        $builder = new Query\Builder($this, $this->getPostProcessor());

        return $builder->executeCql($query);
    }

    /**
     * Run an CQL statement and get the number of rows affected.
     *
     * @param string $query
     * @param array $bindings
     * @return int
     */
    public function affectingStatement($query, $bindings = [])
    {
        // For update or delete statements, we want to get the number of rows affected
        // by the statement and return that back to the developer. We'll first need
        // to execute the statement and then we'll use PDO to fetch the affected.

        foreach ($bindings as $binding) {
            $value = Helper::isAvoidingQuotes($binding)  ? $binding : "'" . $binding . "'";
            $query = preg_replace('/\?/', $value, $query, 1);
        }

        $builder = new Query\Builder($this, $this->getPostProcessor());

        return $builder->executeCql($query);
    }

    /** Execute an CQL statement and return the boolean result.
     * @param string $query
     */
    public function raw($query): Collection
    {
        $builder = new Query\Builder($this, $this->getPostProcessor());

        return $builder->executeCql($query);
    }

    // Interface methods implementation (for Lumen 5.7.* compatibility)

    /**
     * Get a database connection instance.
     *
     * @param string $name
     * @return \Illuminate\Database\ConnectionInterface
     */
    public function connection($name = null)
    {
        return $this;
    }

    /**
     * Get the default connection name.
     *
     * @return string
     */
    public function getDefaultConnection()
    {
        return $this->connection;
    }

    /**
     * Set the default connection name.
     *
     * @param string $name
     * @return void
     */
    public function setDefaultConnection($name)
    {
    }

    /** Create a new Cassandra connection. */
    protected function createConnection(array $config) : DefaultSession
    {
        $cluster = \Cassandra::cluster()
            ->withContactPoints($config['host'])
            ->withPort((int) $config['port']);

        if (!empty($config['authType']) && $config['authType'] == 'userCredentials') {
            if (empty($config['username']) || empty($config['password'])) {
                throw new Exception(
                    "You have selected userCredentials auth type but you have not \n" .
                    'provided username and password, please check your config params'
                );
            }

            $cluster = $cluster->withCredentials($config['username'], $config['password']);
        }

        return $cluster->build()
            ->connect($config['keyspace']);
    }

    /**
     * @inheritdoc
     */
    protected function getDefaultPostProcessor() : Query\Processor
    {
        return new Query\Processor();
    }

    /**
     * @inheritdoc
     */
    protected function getDefaultQueryGrammar()
    {
        return new Query\Grammar();
    }

    /**
     * @inheritdoc
     */
    protected function getDefaultSchemaGrammar()
    {
        return new Schema\Grammar();
    }

    // PRIVATE METHODS

    /**
     * Transform element to UDT update/insert value format
     *
     * @param $elem
     * @return string
     */
    private function elem2UDT($elem)
    {
        $result = '';

        if (is_array($elem)) {
            $result .= '{';
            $each_result = '';
            foreach ($elem as $key => $value) {
                if (!is_array($value)) {
                    $each_result .= "{$key}: '$value',";
                } else {
                    $each_result .= $this->elem2UDT($value) . ',';
                }
            }

            if (substr($each_result, strlen($each_result) - 1) == ',') {
                $each_result = substr($each_result, 0, strlen($each_result) - 1);
            }
            $result .= $each_result;
            $result .= '}';
        }

        return $result;
    }

    /** Check if the configuration cassandra is set in the database.php file */
    private function validateConfig(Application $app) : array
    {
        if (isset($app->make('config')['database.connections.cassandra'])) {
            return $app->make('config')['database.connections.cassandra'];
        }

        throw new Exception(
            "This seems that you missed to add cassandra cofinguration in your database.php file.\n" .
            "Please add the next configuration

            'cassandra' => [
                'driver' => 'cassandra',
                'host' => env('DB_HOST', 'localhost'),
                'port' => env('DB_PORT', 9042),
                'keyspace' => env('DB_DATABASE', 'cassandra_db'),
                'username' => env('DB_USERNAME', ''),
                'password' => env('DB_PASSWORD', ''),
                'authType' => env('DB_AUTH_TYPE', 'userCredentials'),
            ],",
            1
        );
    }
}
