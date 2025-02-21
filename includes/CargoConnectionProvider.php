<?php

use MediaWiki\Config\ServiceOptions;
use Wikimedia\Rdbms\DatabaseFactory;
use Wikimedia\Rdbms\DatabaseMySQL;
use Wikimedia\Rdbms\DBConnRef;
use Wikimedia\Rdbms\IDatabase;
use Wikimedia\Rdbms\ILBFactory;
use Wikimedia\Rdbms\ILoadBalancer;

/**
 * Class to manage access to the Cargo database.
 *
 * By default, this class creates and manages a single connection to the local wiki DB.
 * It can be configured to connect to a different DB using the 'wgCargoDB*' settings,
 * or to eschew manual connection management in favor of MediaWiki's DBAL by setting the 'CargoDBCluster' option.
 */
class CargoConnectionProvider {
	public const CONSTRUCTOR_OPTIONS = [
		// MediaWiki DB setup variables.
		'DBuser',
		'DBpassword',
		'DBport',
		'DBprefix',
		'DBservers',

		// Optional Cargo-specific DB setup variables.
		'CargoDBserver',
		'CargoDBname',
		'CargoDBuser',
		'CargoDBpassword',
		'CargoDBprefix',
		'CargoDBtype',

		// Fandom change: Optional DB index override to use for Cargo in a single-connection setup (PLATFORM-7466)
		'CargoDBIndex',

		// Optional external cluster name to use for Cargo.
		// Supersedes all above configuration if present.
		'CargoDBCluster'
	];

	/**
	 * The database connection to use for accessing Cargo data, if 'CargoDBCluster' is not set.
	 */
	private ?IDatabase $connection = null;

	public function __construct(
		private ILBFactory $lbFactory,
		private DatabaseFactory $databaseFactory,
		private ServiceOptions $serviceOptions
	) {
		$serviceOptions->assertRequiredOptions( self::CONSTRUCTOR_OPTIONS );
	}

	/**
	 * Get a database connection for accessing Cargo data.
	 */
	public function getConnection( int $dbType ): IDatabase {
		$cluster = $this->serviceOptions->get( 'CargoDBCluster' );

		// If a cluster is specified, let MediaWiki's DBAL manage the lifecycle of Cargo-related connections.
		if ( $cluster !== null ) {
			$lb = $this->lbFactory->getExternalLB( $cluster );

			// Fall back to the primary DB if there were recent writes, to ensure that Cargo sees its own changes.
			$dbType = $lb->hasOrMadeRecentPrimaryChanges() ? ILoadBalancer::DB_PRIMARY : $dbType;
			$conn = $lb->getConnection( $dbType );

			// Fandom change: Ensure Cargo DB connections use 4-byte UTF-8 client character set (UGC-4625).
			self::setClientCharacterSet( $conn );
			return $conn;
		}

		if ( $this->connection === null ) {
			$this->connection = $this->initConnection();

			// Fandom change: Ensure Cargo DB connections use 4-byte UTF-8 client character set (UGC-4625).
			self::setClientCharacterSet( $this->connection );
		}

		return $this->connection;
	}

	/**
	 * Get the DB type (e.g. 'postgres') of the Cargo database.
	 * This is mainly useful for code that needs to generate platform-specific SQL.
	 */
	public function getDBType(): string {
		return $this->serviceOptions->get( 'CargoDBtype' ) ?? $this->getConnection( DB_REPLICA )->getType();
	}

	/**
	 * Create a database connection for Cargo data managed entirely by this class.
	 */
	private function initConnection(): IDatabase {
		$lb = $this->lbFactory->getMainLB();
		// Fandom change: Use the DB index specified in the CargoDBIndex option (PLATFORM-7466).
		$index = $this->serviceOptions->get( 'CargoDBIndex' ) ?? $lb::DB_PRIMARY;
		$dbr = $lb->getConnection( $index );

		$dbServers = $this->serviceOptions->get( 'DBservers' );
		$dbUser = $this->serviceOptions->get( 'DBuser' );
		$dbPassword = $this->serviceOptions->get( 'DBpassword' );

		$dbServer = $this->serviceOptions->get( 'CargoDBserver' ) ?? $dbr->getServer();
		$dbName = $this->serviceOptions->get( 'CargoDBname' ) ?? $dbr->getDBname();
		$dbType = $this->serviceOptions->get( 'CargoDBtype' ) ?? $dbr->getType();

		// Server (host), db name, and db type can be retrieved from $dbr via
		// public methods, but username and password cannot. If these values are
		// not set for Cargo, get them from either $wgDBservers or wgDBuser and
		// $wgDBpassword, depending on whether or not there are multiple DB servers.
		$dbUsername = $this->serviceOptions->get( 'CargoDBuser' ) ?? $dbServers[0]['user'] ?? $dbUser;
		$dbPassword = $this->serviceOptions->get( 'CargoDBpassword' ) ?? $dbServers[0]['password'] ?? $dbPassword;
		$dbTablePrefix = $this->serviceOptions->get( 'CargoDBprefix' )
			?? $this->serviceOptions->get( 'DBprefix' ) . 'cargo__';

		$params = [
			'host' => $dbServer,
			'user' => $dbUsername,
			'password' => $dbPassword,
			'dbname' => $dbName,
			'tablePrefix' => $dbTablePrefix,
		];

		if ( $dbType === 'sqlite' ) {
			/** @var \Wikimedia\Rdbms\DatabaseSqlite $dbr */
			$params['dbFilePath'] = $dbr->getDbFilePath();
		} elseif ( $dbType === 'postgres' ) {
			// @TODO - a $wgCargoDBport variable is still needed.
			$params['port'] = $this->serviceOptions->get( 'DBport' );
		}

		return $this->databaseFactory->create( $dbType, $params );
	}

	/**
	 * Set the client character set of a database connection handle to 4-byte UTF-8.
	 * This is necessary because Cargo utilizes functions such as REGEXP_LIKE(),
	 * which fail if the client character set is "binary".
	 */
	private static function setClientCharacterSet( IDatabase $dbw ): void {
		// For DBConnRefs wrapping a lazy-initialized connection handle,
		// we need to unwrap that connection handle to set the character set.
		if ( $dbw instanceof DBConnRef ) {
			// Force open the database connection so that we can obtain the underlying native connection handle.
			$dbw->ping();

			$ref = new ReflectionProperty( $dbw, 'conn' );
			$ref->setAccessible( true );
			$dbw = $ref->getValue( $dbw );
		}

		if ( $dbw instanceof DatabaseMySQL ) {
			// Force open the database connection so that we can obtain the underlying native connection handle.
			$dbw->ping();

			$ref = new ReflectionMethod( $dbw, 'getBindingHandle' );
			$ref->setAccessible( true );

			/** @var mysqli $mysqli */
			$mysqli = $ref->invoke( $dbw );
			if ( $mysqli->character_set_name() !== 'utf8mb4' ) {
				$mysqli->set_charset( 'utf8mb4' );
			}
		}
	}
}
