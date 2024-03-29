<?php

namespace SmartContact\LaravelGoogleAds\Facades;

use Illuminate\Support\Facades\Facade;

class GoogleBigQuery extends Facade
{
    protected static function getFacadeAccessor() { return 'google-bigquery'; }
}
