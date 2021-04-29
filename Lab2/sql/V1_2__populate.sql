INSERT INTO Location (TerName, AreaName, RegName)
SELECT DISTINCT TerName, AreaName, RegName FROM ZNO
UNION SELECT DISTINCT EOTerName, EOAreaName, EORegName FROM ZNO
UNION SELECT DISTINCT ukrPTTerName, ukrPTAreaName, ukrPTRegName FROM ZNO
UNION SELECT DISTINCT mathPTTerName, mathPTAreaName, mathPTRegName FROM ZNO
UNION SELECT DISTINCT physPTTerName, physPTAreaName, physPTRegName FROM ZNO
UNION SELECT DISTINCT engPTTerName, engPTAreaName, engPTRegName FROM ZNO
UNION SELECT DISTINCT histPTTerName, histPTAreaName, histPTRegName FROM ZNO
UNION SELECT DISTINCT chemPTTerName, chemPTAreaName, chemPTRegName FROM ZNO
UNION SELECT DISTINCT bioPTTerName, bioPTAreaName, bioPTRegName FROM ZNO
UNION SELECT DISTINCT geoPTTerName, geoPTAreaName, geoPTRegName FROM ZNO
UNION SELECT DISTINCT deuPTTerName, deuPTAreaName, deuPTRegName FROM ZNO
UNION SELECT DISTINCT fraPTTerName, fraPTAreaName, fraPTRegName FROM ZNO
UNION SELECT DISTINCT spaPTTerName, spaPTAreaName, spaPTRegName FROM ZNO;


INSERT INTO Institution (EOName, EOTypeName, EOParent, locid)
SELECT DISTINCT ON (inst.iname) inst.iname, inst.ieotype, inst.ieoparent, Location.locid FROM (
	SELECT DISTINCT EOName, EOTerName, EOAreaName, EORegName, EOTypeName, EOParent FROM ZNO
	UNION SELECT DISTINCT ukrPTName, ukrPTTerName, ukrPTAreaName, ukrPTRegName, EOTypeName, EOParent FROM ZNO
	UNION SELECT DISTINCT mathPTName, mathPTTerName, mathPTAreaName, mathPTRegName, EOTypeName, EOParent FROM ZNO
	UNION SELECT DISTINCT physPTName, physPTTerName, physPTAreaName, physPTRegName, EOTypeName, EOParent FROM ZNO
	UNION SELECT DISTINCT engPTName, engPTTerName, engPTAreaName, engPTRegName, EOTypeName, EOParent FROM ZNO
	UNION SELECT DISTINCT histPTName, histPTTerName, histPTAreaName, histPTRegName, EOTypeName, EOParent FROM ZNO
	UNION SELECT DISTINCT chemPTName, chemPTTerName, chemPTAreaName, chemPTRegName, EOTypeName, EOParent FROM ZNO
	UNION SELECT DISTINCT bioPTName, bioPTTerName, bioPTAreaName, bioPTRegName, EOTypeName, EOParent FROM ZNO
	UNION SELECT DISTINCT geoPTName, geoPTTerName, geoPTAreaName, geoPTRegName, EOTypeName, EOParent FROM ZNO
	UNION SELECT DISTINCT deuPTName, deuPTTerName, deuPTAreaName, deuPTRegName, EOTypeName, EOParent FROM ZNO
	UNION SELECT DISTINCT fraPTName, fraPTTerName, fraPTAreaName, fraPTRegName, EOTypeName, EOParent FROM ZNO
	UNION SELECT DISTINCT spaPTName, spaPTTerName, spaPTAreaName, spaPTRegName, EOTypeName, EOParent FROM ZNO
) AS inst (iname, iter, iarea, ireg, ieotype, ieoparent)
LEFT JOIN Location ON
	Location.TerName = inst.iter AND
	Location.AreaName = inst.iarea AND
	Location.RegName = inst.ireg
WHERE inst.iname IS NOT NULL;


INSERT INTO Participant (OutID, Birth, SexTypeName, RegTypeName, ClassProfileName, ClassLangName, EOName, locid)
SELECT DISTINCT ON (OutID) OutID, Birth, SexTypeName, RegTypeName,
						   ClassProfileName, ClassLangName, EOName, Location.locid
FROM ZNO
JOIN Location ON
    ZNO.TerName = Location.TerName AND
    ZNO.AreaName = Location.AreaName AND
    ZNO.RegName = Location.RegName;



INSERT INTO Test (TestName, TestStatus, AdaptScale, OutID, EOName, Year, Ball, Ball100, Ball12)
SELECT UkrTest, UkrTestStatus, UkrAdaptScale, OutID, ukrPTName, Year, UkrBall, UkrBall100, UkrBall12
FROM ZNO
WHERE zno.UkrTest IS NOT NULL;

INSERT INTO Test (TestName, Lang, TestStatus, OutID, EOName, Year, Ball, Ball100, Ball12)
SELECT mathTest, mathLang, mathTestStatus, OutID, mathPTName, Year, mathBall, mathBall100, mathBall12
FROM ZNO
WHERE zno.MathTest IS NOT NULL;

INSERT INTO Test (TestName, Lang, TestStatus, OutID, EOName, Year, Ball, Ball100, Ball12)
SELECT physTest, physLang, physTestStatus, OutID, physPTName, Year, physBall, physBall100, physBall12
FROM ZNO
WHERE zno.PhysTest IS NOT NULL;

INSERT INTO Test (TestName, TestStatus, OutID, EOName, Year, Ball, Ball100, Ball12)
SELECT engTest, engTestStatus, OutID, engPTName, Year, engBall, engBall100, engBall12
FROM ZNO
WHERE zno.EngTest IS NOT NULL;

INSERT INTO Test (TestName, Lang, TestStatus, OutID, EOName, Year, Ball, Ball100, Ball12)
SELECT histTest, histLang, histTestStatus, OutID, histPTName, Year, histBall, histBall100, histBall12
FROM ZNO
WHERE zno.HistTest IS NOT NULL;

INSERT INTO Test (TestName, Lang, TestStatus, OutID, EOName, Year, Ball, Ball100, Ball12)
SELECT chemTest, chemLang, chemTestStatus, OutID, chemPTName, Year, chemBall, chemBall100, chemBall12
FROM ZNO
WHERE zno.ChemTest IS NOT NULL;

INSERT INTO Test (TestName, Lang, TestStatus, OutID, EOName, Year, Ball, Ball100, Ball12)
SELECT bioTest, bioLang, bioTestStatus, OutID, bioPTName, Year, bioBall, bioBall100, bioBall12
FROM ZNO
WHERE zno.BioTest IS NOT NULL;

INSERT INTO Test (TestName, Lang, TestStatus, OutID, EOName, Year, Ball, Ball100, Ball12)
SELECT geoTest, geoLang, geoTestStatus, OutID, geoPTName, Year, geoBall, geoBall100, geoBall12
FROM ZNO
WHERE zno.GeoTest IS NOT NULL;

INSERT INTO Test (TestName, TestStatus, OutID, EOName, Year, Ball, Ball100, Ball12)
SELECT deuTest, deuTestStatus, OutID, deuPTName, Year, deuBall, deuBall100, deuBall12
FROM ZNO
WHERE zno.DeuTest IS NOT NULL;

INSERT INTO Test (TestName, TestStatus, OutID, EOName, Year, Ball, Ball100, Ball12)
SELECT fraTest, fraTestStatus, OutID, fraPTName, Year, fraBall, fraBall100, fraBall12
FROM ZNO
WHERE zno.FraTest IS NOT NULL;

INSERT INTO Test (TestName, TestStatus, OutID, EOName, Year, Ball, Ball100, Ball12)
SELECT spaTest, spaTestStatus, OutID, spaPTName, Year, spaBall, spaBall100, spaBall12
FROM ZNO
WHERE zno.SpaTest IS NOT NULL;