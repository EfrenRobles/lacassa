<?php

namespace Cubettech\Lacassa;

use Illuminate\Support\ServiceProvider;
use Cassandra;

class CassandraServiceProvider extends ServiceProvider
{
    /**
     * Bootstrap the application services.
     *
     * @return void
     */
    public function boot()
    {
        //require __DIR__ . '/../vendor/autoload.php';
    }

    /**
     * Register the application services.
     *
     * @return void
     */
    public function register()
    {
        // Add database driver.
	$this->app->singleton('db', function ($app) {
            $config = [
            	'host' => env('DB_HOST', 'localhost'),
            	'port' => env('DB_PORT', 9042),
            	'keyspace' => env('DB_KEYSPACE', 'mykeyspace'),
            	'username' => env('DB_USERNAME', ''),
            	'password' => env('DB_PASSWORD', ''),
            ];
            return new Connection($config);
        });

    }
}
