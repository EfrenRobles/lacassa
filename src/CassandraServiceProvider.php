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
            $config = $app->make('config')["database"]["connections"]["cassandra"];
            return new Connection($config);
        });
    }
}
