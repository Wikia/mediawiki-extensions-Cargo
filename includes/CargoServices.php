<?php

use MediaWiki\MediaWikiServices;

/**
 * Typed service locator class for Cargo service classes.
 */
class CargoServices {
	public static function getCargoConnectionProvider(): CargoConnectionProvider {
		return MediaWikiServices::getInstance()->getService( 'CargoConnectionProvider' );
	}
}
