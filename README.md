## **Eloquent and query builder for Cassandra on Lumen 8.0 with support for PHP 7.4.12 ** (BETA)

A Query builder with support for Cassandra, using the original Laravel API. This library extends the original Laravel classes, so it uses exactly the same methods.

## **Table of contents**

    * Installation
    * Configuration
    * Migration (pending)
    * Eloquent (beta)
    * Models (beta)
    * Seeders (beta)
    * Query Builder
    * Examples

## **Installation**

    Make sure you have the DataStax PHP Driver for Apache Cassandra installed in your PHP container or server.

    You can find installation instructions at https://github.com/datastax/php-driver
    or 
    https://github.com/datastax/php-driver/blob/master/ext/README.md

    *datastax php-driver requires php version 5.6+ but it is possible to install it on php 7.4 using repository PHP-232*

    Installation using composer:

        composer require cubettech/lacassa

    Add the line below in the "Register Service Provider" of bootstrap/app.php file:

        $app->register(Cubettech\Lacassa\CassandraServiceProvider::class);

    Include to .env file the filled data below:

        DB_CONNECTION=cassandra
        DB_HOST=your-docker-container-name
        DB_PORT=9042
        DB_DATABASE=your-keyspace-name
        DB_USERNAME=cassandra-user-name (no tested yet)
        DB_PASSWORD=cassandra-password (no tested yet)
        BB_AUTH_TYPE= empty or userCredentials (no tested yet)


## **Configuration**

    And add a new cassandra connection in your config/database.php:

        'cassandra' => [
            'driver' => 'cassandra',
            'host' => env('DB_HOST', 'localhost'),
            'port' => env('DB_PORT', 9042),
            'keyspace' => env('DB_DATABASE', 'cassandra_db'),
            'username' => env('DB_USERNAME', ''),
            'password' => env('DB_PASSWORD', ''),
            'authType' => env('DB_AUTH_TYPE', ''),
        ],


### **Auth** (No tested yet)

    You can use Laravel's native Auth functionality for cassandra, make sure your config/auth.php looks like 

        'providers' => [
        // 'users' => [
        //     'driver' => 'eloquent',
        //     'model' => App\User::class,
        // ],
        'users' => [
            'driver' => 'database',
            'table' => 'users',
        ],
            ],
### **Migrations** (Pending)

    We can not use migration yet due ( Lost connection and no reconnector available. )

    Preparing the schema in Cubettech\Lacassa\PrepareSchema

    The database driver also has (limited) schema builder support. You can easily manipulate tables and set indexes:

        Schema::create(
            'users', function ($table) {
                $table->int('id');
	            $table->text('name');
	            $table->text('email');
	            $table->text('password');
                $table->text('remember_token');
	            $table->setCollection('phn', 'bigint');
                $table->listCollection('hobbies', 'text');
                $table->mapCollection('friends', 'text', 'text');
                $table->primary(['id']);
          });

    DROP table

        Schema::drop('users');

    # **CQL data types supported**  (No tested yet)

    text('a')
    bigint('b')
    blob('c')
    boolean('d')
    counter('e')
    decimal('f')
    double('g')
    float('h')
    frozen('i')
    inet('j')
    nt('k')
    listCollection('l', 'text')
    mapCollection('m', 'timestamp', 'text')
    setCollection('n', 'int')
    timestamp('o')
    timeuuid('p')
    tuple('q', 'int', 'text', 'timestamp')
    uuid('r')
    varchar('s')
    varint('t')
    ascii('u')

    **Primary Key**
    primary('a')
    primary(['a', 'b'])

**Eloquent** (Beta)

    To use Eloquent on casandra is mandatory enable Eloquent first in the bootstrap/app.php
    Facades are optional if you can avoid this, your app will be better
    
    // $app->withFacades();
    $app->withEloquent();

    $query = (new User)->where('email', '=', 'email@example.com')->get();

    $query = (new User)->first();

**Models** (beta)
    
    In a new file that will be your model you need to use
        use Cubettech\Lacassa\Eloquent\Model;
        
    Instead
        use Illuminate\Database\Eloquent\Model;
    
    in the beta stage, you need to set timestamp = false due this issue ( The column names contains duplicates ), but timestamp like created_at and updated_at will be needed on table.
        public $timestamps = false;

**Seeders** (beta)

    Seedes in cassandra does not have changes.
    
    Model Factories work normal, no changes here, just point to the model that you need.
    
        public function definition()
        {
            return [
                'name' => $this->faker->name,
                'email' => $this->faker->unique()->safeEmail,
            ];
        }
        
    Seeder file work normal, no changes needed here.
    
        class UserTableSeeder extends Seeder
        {
            /**
             * Run the database seeds.
             *
             * @return void
             */
            public function run()
            {
                factory(User::class, 10)
                    ->create();
            }
        }    

**Query Builder** (beta)

    The database driver plugs right into the original query builder. When using cassandra connections, you will be able to build fluent queries to perform database operations.

        $emp = DB::table('emp')->get();

        $emp = DB::table('emp')->where('emp_name', 'Christy')->first();

    If you did not change your default database connection, you will need to specify it when querying.

        $emp = DB::connection('cassandra')->table('emp')->get();

**Examples**

    ### **Basic Usage**

    **Retrieving All Records**

        $emp = DB::table('emp')->all();

    **Indexing columns**  (No tested yet)

    CREATE INDEX creates a new index on the given table for the named column.

        DB::table('users')->index(['name']);

    **Selecting columns**  (ok)

        $emp = DB::table('emp')->where('emp_no', '>', 50)->select('emp_name', 'emp_no')->get();

        $emp = DB::table('emp')->where('emp_no', '>', 50)->get(['emp_name', 'emp_no']);

    **Wheres**

    The WHERE clause specifies which rows to query. In the WHERE clause, refer to a column using the actual name, not an alias. Columns in the WHERE clause need to meet one of these requirements:

    * The partition key definition includes the column.	

    * A column that is indexed using CREATE INDEX.

            $emp = DB::table('emp')->where('emp_no', '>', 50)->take(10)->get();

    **And Statements**

        $emp = DB::table('emp')->where('emp_no', '>', 50)->where('emp_name', '=', 'Christy')->get();

    **Using Where In With An Array**

        $emp = DB::table('emp')->whereIn('emp_no', [12, 17, 21])->get();

    **Order By**

    ORDER BY clauses can select a single column only. Ordering can be done in ascending or descending order, default ascending, and specified with the ASC or DESC keywords. In the ORDER BY clause, refer to a column using the actual name, not the aliases.

        $emp = DB::table('emp')->where('emp_name','Christy')->orderBy('emp_no', 'desc')->get();

    **Limit**

    We can use limit() and take() for limiting the query.

        $emp = DB::table('emp')->where('emp_no', '>', 50)->take(10)->get();

        $emp = DB::table('emp')->where('emp_no', '>', 50)->limit(10)->get();

    **Distinct**

    Distinct requires a field for which to return the distinct values.

        $emp = DB::table('emp')->distinct()->get(['emp_id']);

    Distinct can be combined with **where**:

        $emp = DB::table('emp')->where('emp_sal', 45000)->distinct()->get(['emp_name']);

    **Count**

        $number = DB::table('emp')->count();

    Count can be combined with **where**:

        $sal = DB::table('emp')->where('emp_sal', 45000)->count();

    **Truncate**

        $sal = DB::table('emp')->truncate();

    ### **Filtering a collection set, list, or map**

    You can index the collection column, and then use the CONTAINS condition in the WHERE clause to filter the data for a particular value in the collection.

        $emp = DB::table('emp')->where('emp_name','contains', 'Christy')->get();

    After [indexing the collection keys](https://docs.datastax.com/en/cql/3.1/cql/cql_reference/create_index_r.html#reference_ds_eqm_nmd_xj__CreatIdxCollKey) in the venues map, you can filter on map keys.

        $emp = DB::table(emp')->where('todo','contains key', '2014-10-02 06:30:00+0000')->get();

    **Raw Query**

    The CQL expressions can be injected directly into the query.

        $emp = DB::raw('select * from emp');

    **Inserts, updates and deletes**

    Inserting, updating and deleting records works just like the original QB.

    **Insert**  (ok)

        DB::table('emp')->insert(['emp_id' => 11, 'emp_city' => '{"kochi", "tvm", "kollam"}', 'emp_name' => 'Christy', 'emp_phone' => 12345676890, 'emp_sal' => 500]);

    **Updating**

    To update a model, you may retrieve it, change an attribute, and use the update method.

        DB::table('emp')->where('emp_id', 11)->update(['emp_city' => 'kochi', 'emp_name' => 'Christy jos', 'emp_phone' =>  1234567890]);

    ### **Updating a collection set, list, and map**

    Update collections in a row. The method will be like

        updateCollection(collection_type, column_name, operator, value);

    Collection_type is any of set, list or map.

    Column_name is the name of column to be updated.

    Operator is + or -, + for adding the values to collection and - to remove the value from collection.

    Value can be associative array for map type and array of string/number for list and set types.

        DB::table('users')->where('id', 1)->updateCollection('set', 'phn', '+', [123, 1234,12345])->update();

        DB::table('users')->where('id', 1)->updateCollection('set', 'phn', '-', [123])->update();

        DB::table('users')->where('id', 1)->updateCollection('list', 'hobbies', '+', ['reading', 'cooking', 'cycling'])->update();

        DB::table('users')->where('id', 1)->updateCollection('list', 'hobbies', '-', ['cooking'])->update();

        DB::table('users')->where('id', 1)->updateCollection('map', 'friends', '+', ['John' => 'Male', 'Rex' => 'Male'])->update();

        DB::table('users')->where('id', 1)->updateCollection('map', 'friends', '-', ['John'])->update();

    **Deleting**  (ok at rows)

    To delete a model, simply call the delete method on the instance. We can delete the rows in a table by using deleteRow method:

        $emp = DB::table('emp')->where('emp_city', 'Kochi')->deleteRow();

    We can also perform delete by the column in a table using deleteColumn method:

        $emp = DB::table('emp')->where('emp_id', 3)->deleteColumn();
