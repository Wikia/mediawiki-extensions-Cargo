<?php

use MediaWiki\Config\ServiceOptions;
use MediaWiki\MediaWikiServices;

return [
	'CargoConnectionProvider' => static function ( MediaWikiServices $services ): CargoConnectionProvider {
		// DatabaseFactory only exists on MW 1.39 and newer.
		$databaseFactory = $services->hasService( 'DatabaseFactory' ) ? $services->getDatabaseFactory() : null;
		return new CargoConnectionProvider(
			$services->getDBLoadBalancerFactory(),
			$databaseFactory,
			new ServiceOptions( CargoConnectionProvider::CONSTRUCTOR_OPTIONS, $services->getMainConfig() )
		);
	}
];
