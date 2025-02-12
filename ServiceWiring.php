<?php

use MediaWiki\Config\ServiceOptions;
use MediaWiki\MediaWikiServices;

return [
	CargoConnectionProvider::class => static function ( MediaWikiServices $services ): CargoConnectionProvider {
		return new CargoConnectionProvider(
			$services->getDBLoadBalancerFactory(),
			$services->getDatabaseFactory(),
			new ServiceOptions( CargoConnectionProvider::CONSTRUCTOR_OPTIONS, $services->getMainConfig() )
		);
	}
];
