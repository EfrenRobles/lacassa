<?php

namespace Cubettech\Lacassa;

use Illuminate\Support\ServiceProvider;

class CassandraServiceProvider extends ServiceProvider
{
    /** Bootstrap the application services. */
    public function boot() : void
    {
        //require __DIR__ . '/../vendor/autoload.php';
    }

    /** Register the application services, add database driver. */
    public function register() : void
    {
        $this->app->singleton('db', function ($app) {
            return new Connection($app);
        });
    }
}
