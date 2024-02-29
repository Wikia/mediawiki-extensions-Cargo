<?php

use MediaWiki\Config\ServiceOptions;
use Wikimedia\Rdbms\DatabaseFactory;
use Wikimedia\Rdbms\IDatabase;
use Wikimedia\Rdbms\ILBFactory;
use Wikimedia\Rdbms\ILoadBalancer;

/**
 * @covers CargoConnectionProvider
 */
class CargoConnectionProviderUnitTest extends MediaWikiUnitTestCase {

	private ILBFactory $lbFactory;

	private ILoadBalancer $dbLoadBalancer;

	private DatabaseFactory $databaseFactory;

	private IDatabase $connection;

	protected function setUp(): void {
		parent::setUp();
		if ( !class_exists( DatabaseFactory::class ) ) {
			$this->markTestSkipped( 'The DatabaseFactory class is not available' );
		}

		$this->lbFactory = $this->createMock( ILBFactory::class );
		$this->dbLoadBalancer = $this->createMock( ILoadBalancer::class );
		$this->databaseFactory = $this->createMock( DatabaseFactory::class );
		$this->connection = $this->createMock( IDatabase::class );

		$this->lbFactory->expects( $this->any() )
			->method( 'getMainLB' )
			->willReturn( $this->dbLoadBalancer );
	}

	/**
	 * @dataProvider provideConnectionConfigs
	 */
	public function testShouldCreateAndManageConnectionBasedOnConfigVars(
		array $config,
		string $expectedDbType,
		array $expectedConnectionParams
	): void {
		$serviceOptions = new ServiceOptions( CargoConnectionProvider::CONSTRUCTOR_OPTIONS, $config );
		$connectionProvider = new CargoConnectionProvider( $this->lbFactory, $this->databaseFactory, $serviceOptions );
		$mainConn = $this->createMock( IDatabase::class );

		$this->dbLoadBalancer->expects( $this->any() )
			->method( 'getConnection' )
			->with( $serviceOptions->get( 'CargoDBIndex' ) ?? DB_PRIMARY )
			->willReturn( $mainConn );

		$mainConn->expects( $this->any() )
			->method( 'getServer' )
			->willReturn( 'localhost' );
		$mainConn->expects( $this->any() )
			->method( 'getDBname' )
			->willReturn( 'test_wiki_db' );
		$mainConn->expects( $this->any() )
			->method( 'getType' )
			->willReturn( 'mysql' );

		$this->databaseFactory->expects( $this->once() )
			->method( 'create' )
			->with( $expectedDbType, $expectedConnectionParams )
			->willReturn( $this->connection );

		$replicaConn = $connectionProvider->getConnection( DB_REPLICA );
		$primaryConn = $connectionProvider->getConnection( DB_PRIMARY );

		$this->assertSame( $this->connection, $replicaConn );
		$this->assertSame( $this->connection, $primaryConn );
	}

	public static function provideConnectionConfigs(): iterable {
		yield 'inferred from main connection' => [
			[
				'DBuser' => 'db_user',
				'DBpassword' => 'db_password',
				'DBport' => 0,
				'DBprefix' => '',
				'DBservers' => [],

				'CargoDBserver' => null,
				'CargoDBname' => null,
				'CargoDBuser' => null,
				'CargoDBpassword' => null,
				'CargoDBprefix' => null,
				'CargoDBtype' => null,

				'CargoDBIndex' => null,

				'CargoDBCluster' => null,
			],
			'mysql',
			[
				'host' => 'localhost',
				'user' => 'db_user',
				'password' => 'db_password',
				'dbname' => 'test_wiki_db',
				'tablePrefix' => 'cargo__',
			]
		];

		yield 'inferred from DBservers' => [
			[
				'DBuser' => 'db_user',
				'DBpassword' => 'db_password',
				'DBport' => 0,
				'DBprefix' => '',
				'DBservers' => [
					[ 'user' => 'primary_db_user', 'password' => 'primary_db_password' ],
					[ 'user' => 'replica_db_user', 'password' => 'replica_db_password' ],
				],

				'CargoDBserver' => null,
				'CargoDBname' => null,
				'CargoDBuser' => null,
				'CargoDBpassword' => null,
				'CargoDBprefix' => null,
				'CargoDBtype' => null,

				'CargoDBIndex' => null,

				'CargoDBCluster' => null,
			],
			'mysql',
			[
				'host' => 'localhost',
				'user' => 'primary_db_user',
				'password' => 'primary_db_password',
				'dbname' => 'test_wiki_db',
				'tablePrefix' => 'cargo__',
			]
		];

		yield 'inferred from main connection with CargoDBIndex override' => [
			[
				'DBuser' => 'db_user',
				'DBpassword' => 'db_password',
				'DBport' => 0,
				'DBprefix' => '',
				'DBservers' => [],

				'CargoDBserver' => null,
				'CargoDBname' => null,
				'CargoDBuser' => null,
				'CargoDBpassword' => null,
				'CargoDBprefix' => null,
				'CargoDBtype' => null,

				'CargoDBIndex' => DB_REPLICA,

				'CargoDBCluster' => null,
			],
			'mysql',
			[
				'host' => 'localhost',
				'user' => 'db_user',
				'password' => 'db_password',
				'dbname' => 'test_wiki_db',
				'tablePrefix' => 'cargo__',
			]
		];

		yield 'inferred from CargoDB overrides' => [
			[
				'DBuser' => 'db_user',
				'DBpassword' => 'db_password',
				'DBport' => 0,
				'DBprefix' => '',
				'DBservers' => [
					[ 'user' => 'primary_db_user', 'password' => 'primary_db_password' ],
					[ 'user' => 'replica_db_user', 'password' => 'replica_db_password' ],
				],

				'CargoDBserver' => 'cargodbhost',
				'CargoDBname' => 'cargo_db_name',
				'CargoDBuser' => 'cargo_db_user',
				'CargoDBpassword' => 'cargo_db_password',
				'CargoDBprefix' => 'cargoprefix',
				'CargoDBtype' => 'postgres',

				'CargoDBIndex' => null,

				'CargoDBCluster' => null,
			],
			'postgres',
			[
				'host' => 'cargodbhost',
				'user' => 'cargo_db_user',
				'password' => 'cargo_db_password',
				'dbname' => 'cargo_db_name',
				'tablePrefix' => 'cargoprefix',
				'port' => 0,
			]
		];

		yield 'inferred from a subset of CargoDB overrides' => [
			[
				'DBuser' => 'db_user',
				'DBpassword' => 'db_password',
				'DBport' => 0,
				'DBprefix' => '',
				'DBservers' => [
					[ 'user' => 'primary_db_user', 'password' => 'primary_db_password' ],
					[ 'user' => 'replica_db_user', 'password' => 'replica_db_password' ],
				],

				'CargoDBserver' => 'cargodbhost',
				'CargoDBname' => 'cargo_db_name',
				'CargoDBuser' => null,
				'CargoDBpassword' => null,
				'CargoDBprefix' => null,
				'CargoDBtype' => null,

				'CargoDBIndex' => null,

				'CargoDBCluster' => null,
			],
			'mysql',
			[
				'host' => 'cargodbhost',
				'user' => 'primary_db_user',
				'password' => 'primary_db_password',
				'dbname' => 'cargo_db_name',
				'tablePrefix' => 'cargo__',
			]
		];
	}

	/**
	 * @dataProvider provideClusterConnectionTypes
	 */
	public function testShouldObtainConnectionFromMediaWikiLoadBalancerIfClusterOptionSet( int $dbType ): void {
		$serviceOptions = new ServiceOptions( CargoConnectionProvider::CONSTRUCTOR_OPTIONS, [
			'DBuser' => 'db_user',
			'DBpassword' => 'db_password',
			'DBport' => 0,
			'DBprefix' => '',
			'DBservers' => [],

			'CargoDBserver' => null,
			'CargoDBname' => null,
			'CargoDBuser' => null,
			'CargoDBpassword' => null,
			'CargoDBprefix' => null,
			'CargoDBtype' => null,

			'CargoDBIndex' => null,

			'CargoDBCluster' => 'testCargoCluster',
		] );

		$connectionProvider = new CargoConnectionProvider( $this->lbFactory, $this->databaseFactory, $serviceOptions );
		$cargoLoadBalancer = $this->createMock( ILoadBalancer::class );
		$cargoConn = $this->createMock( IDatabase::class );

		$cargoLoadBalancer->expects( $this->any() )
			->method( 'hasOrMadeRecentPrimaryChanges' )
			->willReturn( false );

		$cargoLoadBalancer->expects( $this->any() )
			->method( 'getConnection' )
			->with( $dbType )
			->willReturn( $cargoConn );

		$this->lbFactory->expects( $this->any() )
			->method( 'getExternalLB' )
			->with( 'testCargoCluster' )
			->willReturn( $cargoLoadBalancer );

		$conn = $connectionProvider->getConnection( $dbType );

		$this->assertSame( $cargoConn, $conn );
	}

	/**
	 * @dataProvider provideClusterConnectionTypes
	 */
	public function testShouldObtainPrimaryConnectionFromMediaWikiLoadBalancerIfClusterOptionSetWithRecentWrites(
		int $dbType
	): void {
		$serviceOptions = new ServiceOptions( CargoConnectionProvider::CONSTRUCTOR_OPTIONS, [
			'DBuser' => 'db_user',
			'DBpassword' => 'db_password',
			'DBport' => 0,
			'DBprefix' => '',
			'DBservers' => [],

			'CargoDBserver' => null,
			'CargoDBname' => null,
			'CargoDBuser' => null,
			'CargoDBpassword' => null,
			'CargoDBprefix' => null,
			'CargoDBtype' => null,

			'CargoDBIndex' => null,

			'CargoDBCluster' => 'testCargoCluster',
		] );

		$connectionProvider = new CargoConnectionProvider( $this->lbFactory, $this->databaseFactory, $serviceOptions );
		$cargoLoadBalancer = $this->createMock( ILoadBalancer::class );
		$cargoConn = $this->createMock( IDatabase::class );

		$cargoLoadBalancer->expects( $this->any() )
			->method( 'hasOrMadeRecentPrimaryChanges' )
			->willReturn( true );

		$cargoLoadBalancer->expects( $this->any() )
			->method( 'getConnection' )
			->with( DB_PRIMARY )
			->willReturn( $cargoConn );

		$this->lbFactory->expects( $this->any() )
			->method( 'getExternalLB' )
			->with( 'testCargoCluster' )
			->willReturn( $cargoLoadBalancer );

		$conn = $connectionProvider->getConnection( $dbType );

		$this->assertSame( $cargoConn, $conn );
	}

	public static function provideClusterConnectionTypes(): iterable {
		yield 'replica DB' => [ DB_REPLICA ];
		yield 'primary DB' => [ DB_PRIMARY ];
	}
}
