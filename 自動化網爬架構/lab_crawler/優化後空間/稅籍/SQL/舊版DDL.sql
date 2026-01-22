-- CrawlerTestDB.TaxRecord definition

CREATE TABLE `TaxRecord` (
  `Party_ID` varchar(20) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `Insert_Time` datetime DEFAULT NULL,
  `COM_1_TYPE` char(1) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,
  `COM_1_Update_Time` datetime DEFAULT NULL,
  `COM_3_Update_Time` datetime DEFAULT NULL,
  `COM_3_TYPE` char(1) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,
  `BIZ_TYPE` char(1) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,
  `COM_TYPE` char(1) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,
  `BNH_TYPE` char(1) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,
  `COM_D_TYPE` char(1) CHARACTER SET latin1 COLLATE latin1_general_ci DEFAULT NULL,
  `COM_D_Update_Time` datetime DEFAULT NULL,
  PRIMARY KEY (`Party_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- CrawlerTestDB.TaxInfo definition

CREATE TABLE `TaxInfo` (
  `Party_ID` varchar(20) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `Party_Addr` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `Parent_Party_ID` int DEFAULT NULL,
  `Party_Name` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `PaidIn_Capital` bigint DEFAULT NULL,
  `Setup_Date` date DEFAULT NULL,
  `Party_Type` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Use_Invoice` char(1) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Ind_Code` int DEFAULT NULL,
  `Ind_Name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Ind_Code1` int DEFAULT NULL,
  `Ind_Name1` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Ind_Code2` int DEFAULT NULL,
  `Ind_Name2` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Ind_Code3` int DEFAULT NULL,
  `Ind_Name3` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Update_Time` datetime DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- CrawlerTestDB.Tmp_TaxInfo definition

CREATE TABLE `Tmp_TaxInfo` (
  `Party_ID` varchar(20) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `Party_Addr` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `Parent_Party_ID` int DEFAULT NULL,
  `Party_Name` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `PaidIn_Capital` bigint DEFAULT NULL,
  `Setup_Date` date DEFAULT NULL,
  `Party_Type` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Use_Invoice` char(1) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Ind_Code` int DEFAULT NULL,
  `Ind_Name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Ind_Code1` int DEFAULT NULL,
  `Ind_Name1` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Ind_Code2` int DEFAULT NULL,
  `Ind_Name2` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Ind_Code3` int DEFAULT NULL,
  `Ind_Name3` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Update_Time` datetime DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- CrawlerTestDB.NEWTaxInfo definition

CREATE TABLE `NEWTaxInfo` (
  `Party_ID` varchar(20) CHARACTER SET latin1 COLLATE latin1_general_ci NOT NULL,
  `Party_Addr` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `Parent_Party_ID` int DEFAULT NULL,
  `Party_Name` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `PaidIn_Capital` bigint DEFAULT NULL,
  `Setup_Date` date DEFAULT NULL,
  `Party_Type` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Use_Invoice` char(1) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Ind_Code` int DEFAULT NULL,
  `Ind_Name` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Ind_Code1` int DEFAULT NULL,
  `Ind_Name1` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Ind_Code2` int DEFAULT NULL,
  `Ind_Name2` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Ind_Code3` int DEFAULT NULL,
  `Ind_Name3` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `Update_Time` datetime DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;