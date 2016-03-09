
/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


# Dump of table column_type_test
# ------------------------------------------------------------

DROP TABLE IF EXISTS `column_type_test`;

CREATE TABLE `column_type_test` (
  `Id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `SmallIntColumn` smallint(6) DEFAULT NULL,
  `TinyIntColumn` tinyint(4) DEFAULT NULL,
  `BigIntColumn` bigint(20) DEFAULT NULL,
  `FloatColumn` float DEFAULT NULL,
  `DoubleColumn` double DEFAULT NULL,
  `DecimalColumn` decimal(10,6) DEFAULT NULL,
  `BitColumn` bit(1) DEFAULT NULL,
  `BooleanColumn` tinyint(1) DEFAULT NULL,
  `CharColumn` char(1) DEFAULT NULL,
  `VarcharColumn` varchar(10) DEFAULT NULL,
  `DateColumn` date DEFAULT NULL,
  `DateTimeColumn` datetime DEFAULT NULL,
  `TimeStampColumn` timestamp NULL DEFAULT NULL,
  `TimeColumn` time DEFAULT NULL,
  PRIMARY KEY (`Id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

LOCK TABLES `column_type_test` WRITE;
/*!40000 ALTER TABLE `column_type_test` DISABLE KEYS */;

INSERT INTO `column_type_test` (`Id`, `SmallIntColumn`, `TinyIntColumn`, `BigIntColumn`, `FloatColumn`, `DoubleColumn`, `DecimalColumn`, `BitColumn`, `BooleanColumn`, `CharColumn`, `VarcharColumn`, `DateColumn`, `DateTimeColumn`, `TimeStampColumn`, `TimeColumn`)
VALUES
	(10009812,1,1,200999121231212,100.2,12.0123,17.501200,00000001,1,'a','good boy','2016-02-03','2016-02-03 11:50:42','2016-02-03 11:51:01','11:50:47');

/*!40000 ALTER TABLE `column_type_test` ENABLE KEYS */;
UNLOCK TABLES;



/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
