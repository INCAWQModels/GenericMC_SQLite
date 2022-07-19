using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace MC
{
    class ParameterArrayHeader
    {
        public StringBuilder header;

        public int write(string fileName)
        {
            try
            {
                using (StreamWriter writer = new StreamWriter(fileName, true))
                {
                    writer.WriteLine(header.ToString());
                }
                return 0;
            }
            catch { return -1; }
        }
    }

    class PERSiSTParameterArrayHeader : ParameterArrayHeader
    {
        public PERSiSTParameterArrayHeader(int landUseClasses, int numberOfReaches, int numberOfBoxes)
        {
            int i, j,k,l;
            header = new StringBuilder();
            header.Append("ID").Append(MCParameters.separatorChar);
            header.Append("TimeSteps").Append(MCParameters.separatorChar);
            header.Append("StartDate").Append(MCParameters.separatorChar);
            header.Append("SoilBoxCount").Append(MCParameters.separatorChar);
            header.Append("BaselineSMD").Append(MCParameters.separatorChar);

            for(j=0;j<landUseClasses;j++) header.Append("Name(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
	        for(j=0;j<landUseClasses;j++) header.Append("SnowMultiplier(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
	        for(j=0;j<landUseClasses;j++) header.Append("SnowMelt(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
	        for(j=0;j<landUseClasses;j++) header.Append("DegreeDayMeltFactor(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
	        for(j=0;j<landUseClasses;j++) header.Append("RainMultiplier(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
	        for(j=0;j<landUseClasses;j++) header.Append("InitialSnowDepth(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
	        for(j=0;j<landUseClasses;j++) header.Append("GrowingDegreeThreshold(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
	        for(j=0;j<landUseClasses;j++) header.Append("DegreeDayEvapotranspiration(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("Interception(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);

            for (k=0; k<numberOfBoxes;k++)
            {
                header.Append("Name(").Append(k.ToString()).Append(MCParameters.separatorChar);
		        for(j=0;j<landUseClasses;j++) header.Append("InitialWaterDepth(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
		        for(j=0;j<landUseClasses;j++) header.Append("RelativeAreaIndex(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
		        for(j=0;j<landUseClasses;j++) header.Append("Infiltration(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
		        for(j=0;j<landUseClasses;j++) header.Append("RetainedWaterDepth(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
		        for(j=0;j<landUseClasses;j++) header.Append("DroughtRunoffFraction(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
		        for(j=0;j<landUseClasses;j++) header.Append("ResidenceTime(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
		        for(j=0;j<landUseClasses;j++) header.Append("EvapotranspirationAdjustment(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
		        for(j=0;j<landUseClasses;j++) header.Append("RelativeEvapotranspirationIndex(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
		        for(j=0;j<landUseClasses;j++) header.Append("FieldCapacity(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
		        for(j=0;j<landUseClasses;j++) header.Append("InundationThreshold(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
		        for(j=0;j<landUseClasses;j++) header.Append("Porosity(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                for (j = 0; j < landUseClasses; j++) header.Append("InfiltrationOffset(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
		        header.Append("IsQuickBox(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
		        header.Append("AllowInfiltration(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("AllowInundation(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
		        header.Append("UseSMD(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
            }
            header.Append("ReachCount").Append(MCParameters.separatorChar);
            for (i = 0; i < numberOfReaches; i++)
            {
                header.Append("ReachName(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("CatchmentArea(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InitialReachFlow(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ReachLength(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Width(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("a(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("b(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("PrecipAsSnow(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ReachSnowMultiplier(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ReachRainMultiplier(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Abstraction(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Effluent(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                for (j = 0; j < landUseClasses; j++) header.Append("LandscapeUnitPct(").Append(j.ToString()).Append(".").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
            }
            for(j=0;j<landUseClasses;j++) header.Append("SquareMatrix(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (k = 0; k < numberOfBoxes; k++)
            {
                for (l = 0; l < numberOfBoxes; l++) header.Append("SM(").Append(k.ToString()).Append(".").Append(l.ToString()).Append(MCParameters.separatorChar);
            }
        }
    }

    class INCA_CParameterArrayHeader : ParameterArrayHeader
    {
        public INCA_CParameterArrayHeader(int landUseClasses, int numberOfReaches)
        {
            int i,j;
            header = new StringBuilder();
            header.Append("ID").Append(MCParameters.separatorChar);
            header.Append("ParameterSetName").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("LandCoverAbbreviation(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("LandCoverName(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DirectRunoffInitialFlow(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerInitialFlow(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerInitialFlow(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DirectRunoffInitialPDC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerInitialSOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerInitialDOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerInitialDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerInitialSOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerInitialDOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerInitialDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DirectRunoffInitialDOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("InitialSoilTemperature(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            header.Append("startDate").Append(MCParameters.separatorChar);
            header.Append("numberOfDays").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerPDCToSOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerPDCToDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerPDCToDOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerSOCToDOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerSOCToDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerDOCToSOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerDOCToDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerSOCToDOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerSOCToDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerDOCToSOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerDOCToDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerCO2MassTransferVelocity(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DailyPDCInputToOrganicLayer(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("FertilizerAdditionStartDay(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("FertilizerAdditionDuration(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("FertilizerAdditionAmount(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MaximumSoilMoistureDeficit(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilTDiffBnSummerAndWinterMax(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SnowDepthFactor(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("COUP_10DegreeResponse(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("COUP_BaseT(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SnowPackInitialDepth(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SnowPackDegreeDayMeltFactor(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SnowPackWaterEquivalent(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilTThermalConductivity(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilTSpecificHeatCapacity(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MinimumOrganicLayerFlow(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MinimumMineralLayerFlow(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerMaxDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerMaxDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerCO2MassTransferVelocity(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerReturnTime(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerEnzymaticLatchRate(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerCriticalSMD(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerReturnTime(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerEnzymaticLatchRate(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerCriticalSMD(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerB1(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerB2(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerB1(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerB2(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
             for (j = 0; j < landUseClasses; j++) header.Append("ZeroRateDepth(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MaxRateDepth(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MaxSMDFraction(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DirectRunoffPDCToDOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerFastPoolFraction(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerPartitioningRate(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerFastPoolFraction(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerPartitioningRate(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DirectRunoffResidenceTime(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerResidenceTime(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerResidenceTime(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerRetentionVolume(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerRetentionVolume(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            header.Append("RiverFlow").Append(MCParameters.separatorChar);
            header.Append("InitialOpenWaterPDC").Append(MCParameters.separatorChar);
            header.Append("InitialOpenWaterDOC").Append(MCParameters.separatorChar);
            header.Append("InitialOpenWaterDIC").Append(MCParameters.separatorChar);
            header.Append("InitialWaterTemperature").Append(MCParameters.separatorChar);
            header.Append("MinimumWaterTemperature").Append(MCParameters.separatorChar);
            header.Append("WaterTemperatureLagFactor").Append(MCParameters.separatorChar);
            header.Append("numberOfReaches").Append(MCParameters.separatorChar);
            for (i=0; i<numberOfReaches; i++)
            {
                header.Append("reachName(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("reachLength(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("averageWidth(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("latitude(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("longitude(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("flow_a(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("flow_b(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ReachArea(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("OpenWaterPDCSettlingVelocity(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("DOCToDICSelfShadingFactor(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("DOCToDICRadiationMultiplier(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("OpenWaterDOCToDICMicrobial(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("OpenWaterPDCToDOC(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("OpenWaterDOCToPDC(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("OpenWaterDICMassTransferRate(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("DICToPDCPhotosynthesis(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("AbstractionRate(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("EffluentInputRate(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("EffluentConcentration(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("LitterFallOntoReach(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("AbstractionOnOffSwitch(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
            }
            for (i = 0; i < numberOfReaches; i++)
            {
                header.Append("CatchmentArea(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                    for (j = 0; j < landUseClasses; j++)
                        header.Append("Land(").Append(i.ToString()).Append("_").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
                    header.Append("BaseFlowIndex(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                    header.Append("ThresholdSoilZoneFlow(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                    header.Append("CO2SaturationConcentration(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                    header.Append("AtmosphericPartialPressureOfCO2(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                    header.Append("RainfallExcessProportion(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                    header.Append("MaximumInfiltrationRate(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                    header.Append("DryDeposition(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                    header.Append("WetDeposition(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);

            }
        }
    }

    class INCA_C4PParameterArrayHeader : ParameterArrayHeader
    {
        public INCA_C4PParameterArrayHeader(int landUseClasses, int numberOfReaches, int SedimentClasses)
        {
            int i, j;
            header = new StringBuilder();
            header.Append("ID").Append(MCParameters.separatorChar);
            header.Append("ParameterSetName").Append(MCParameters.separatorChar);
            header.Append("SedimentClasses").Append(MCParameters.separatorChar);
            for (j = 0; j < SedimentClasses; j++) header.Append("SedimentClassName(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < SedimentClasses; j++) header.Append("MaxGrainSize(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < SedimentClasses; j++) header.Append("MinGrainSize(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < SedimentClasses; j++) header.Append("SedimentDensity(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < SedimentClasses; j++) header.Append("InmanMultiplier(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < SedimentClasses; j++) header.Append("InmanExponent(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            header.Append("LandCoverClasses").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("LandCoverAbbreviation(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("LandCoverName(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DirectRunoffInitialFlow(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerInitialFlow(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerInitialFlow(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DirectRunoffInitialPDC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerInitialSOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerInitialDOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerInitialDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerInitialSOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerInitialDOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerInitialDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SedimentStoreInitialCondition(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            header.Append("startDate").Append(MCParameters.separatorChar);
            header.Append("numberOfDays").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SMDMaxForChemistry(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerMinimumFlow(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("VegetationIndex(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MaximumInfiltrationRate(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilErodibilityForSplashDetachment(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilErodibilityForFlowErosion(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SplashDetachmanr_a_Parameter(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("PlantGrowthStartDay(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("PlantGrowthPeriod(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("InitialSnowDepth(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DegreeDayMeltFactor(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SnowWaterEquivalent(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerPDCToSOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerPDCToDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerPDCToDOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerSOCToDOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerSOCToDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerDOCToSOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerDOCToDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerSOCToDOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerSOCToDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerDOCToSOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerDOCToDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerCO2MassTransferVelocity(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DailyPDCInputToOrganicLayer(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("LitterfallStartDay(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("LitterfallDuration(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("LitterfallAmount(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SnowDepthFactor(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("COUP_10DegreeResponse(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("COUP_BaseT(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilTThermalConductivity(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilTSpecificHeatCapacity(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MinimumMineralLayerFlow(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerMaxDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerMaxDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerCO2MassTransferVelocity(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerReturnTime(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerEnzymaticLatchRate(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerCriticalSMD(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerReturnTime(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerEnzymaticLatchRate(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerCriticalSMD(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerB1(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerB2(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerB1(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerB2(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("Delta(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MaxSMDForOLRetentionVolume(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MaxSMDForMLRetentionVolume(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DirectRunoffTimeConstant(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerTimeConstant(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerTimeConstant(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerTotalToAvailableWaterRatio(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerTotalToAvailableWaterRatio(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            header.Append("RiverFlow").Append(MCParameters.separatorChar);
            header.Append("InitialOpenWaterPDC").Append(MCParameters.separatorChar);
            header.Append("InitialOpenWaterDOC").Append(MCParameters.separatorChar);
            header.Append("InitialOpenWaterDIC").Append(MCParameters.separatorChar);
            header.Append("InitialStreamBedPDC").Append(MCParameters.separatorChar);
            header.Append("InitialStreamBedDOC").Append(MCParameters.separatorChar);
            header.Append("InitialStreamBedDIC").Append(MCParameters.separatorChar);
            header.Append("MinimumWaterTemperature").Append(MCParameters.separatorChar);
            header.Append("numberOfReaches").Append(MCParameters.separatorChar);
            for (i = 0; i < numberOfReaches; i++)
            {
                header.Append("CatchmentArea(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                for (j = 0; j < landUseClasses; j++)
                    header.Append("Land(").Append(i.ToString()).Append("_").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("DeltaMG(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ThresholdSoilZoneFlow(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("RainfallExcessProportion(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("FlowErosionScalingFactor(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("FlowErosionDirectRunoffThreshold(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("FlowErosionNonLinearCoefficient(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("TransportCapacityScalingFactor(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("TransportCapacityDirectRunoffThreshold(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("TransportCapacityNonLinearCoefficient(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("GroundwaterSustainableFlow(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("GroundwaterTimeConstant(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("GroundwaterMaximumEffectiveDepth(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ProportionOfFilledPoreSpace(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("GroundwaterInitialFlow(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("MeanSlope(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("CO2SaturationConcentration(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("AtmosphericPartialPressureOfCO2(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("HenrysLawConstantforCO2(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("DryDeposition(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("WetDeposition(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("GroundwaterInitialSOC(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("GroundwaterInitialDOC(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("GroundwaterInitialDIC(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("DeltaOM(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("DeltaOG(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
            }
            for (i = 0; i < numberOfReaches; i++)
            {
                header.Append("reachName(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("reachID(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("reachLength(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("averageWidth(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("slope(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("flow_a(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("flow_b(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("a7(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("a8(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("a9(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("a10(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("latitude(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("longitude(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ReachArea(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                
                header.Append("OpenWaterPDCSettlingVelocity(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("DOCToDICSelfShadingFactor(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("DOCToDICRadiationMultiplier(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("OpenWaterDOCToDICMicrobial(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("OpenWaterPDCToDOC(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("OpenWaterDOCToPDC(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("OpenWaterDICMassTransferRate(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("DICToPDCPhotosynthesis(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Porosity(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("BedDepth(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("WaterColumnToPoreWaterCExchange(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("StreamBedDOCToPDC(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("StreamBedPDCToDOC(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("StreamBedPDCMineralization(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("StreamBedDOCMineralization(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("AbstractionRate(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("EffluentInputRate(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("EffluentConcentration(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("LitterFallOntoReach(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("EffluentDICConcentraiton(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("AbstractionOnOffSwitch(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
            }
 
        }
    }

    class INCA_PParameterArrayHeader : ParameterArrayHeader
    {
        public INCA_PParameterArrayHeader(int landUseClasses, int numberOfReaches)
        {
            int i, j;
            header = new StringBuilder();
            header.Append("ID").Append(MCParameters.separatorChar);
            header.Append("ParameterSetName").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("LandCoverAbbreviation(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("LandCoverName(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DirectRunoff(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilWaterFlow(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SedimentStore(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilWaterTDP(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilInactiveP(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilLabileP(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DirectRunoffTDP(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DirectRunoffPP(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            header.Append("startDate").Append(MCParameters.separatorChar);
            header.Append("numberOfDays").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SMDMax(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilWaterSustainableFlow(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("VegetationIndex(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("ClayPct(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SiltPct(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("FineSandPct(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MediumSandPct(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("CoarseSandPct(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MaximumInfiltrationRate(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SplashDetachmentSoilErodibilityParameter(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("FlowErosionSoilErodibilityParameter(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SplashDetachment_a(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("PlantGrowthStartDay(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("PlantGrowthPeriod(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("InitialSnowDepth(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DegreeDaySnowmelt(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SWE(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MaximumTDiff(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("FertiliserAdditionStartDay(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("FertiliserAdditionPeriod(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("PlantResidue(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SolidManureAndSlurry(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SolidFertiliser(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("LiquidManureAndSlurry(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("LiquidFertiliser(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SnowDepthSoilTFactor(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("ResponseToTChange(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("BaseTResponse(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("ThermalConductivityOfSoil(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SpecificHeatCapacityDueToFreezeThaw(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("GrowthCurveOffset(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("GrowthCurveAmplitude(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("PlantPUptake(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("ImmobilisationRate(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MaximumUptake(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("ImmobilisationTThreshold(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("FreundlichIsothermConstant(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("WeatheringFactor(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SorptionCoefficientForSoilMatrix(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("EquilibriumPSoilMatrix(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilDepth(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilBulkDensity(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("LabilePSaturationThreshold(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DailyMaximumPUptake(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("EnrichmentRatio(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DirectRunoffTimeConstant(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilReactiveZoneTimeConstant(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilWaterRetentionVolumeConstant(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            header.Append("Flow").Append(MCParameters.separatorChar);
            header.Append("WaterColumnTDP").Append(MCParameters.separatorChar);
            header.Append("StreamBedTDP").Append(MCParameters.separatorChar);
            header.Append("WaterColumnPP").Append(MCParameters.separatorChar);
            header.Append("StreamBedPP").Append(MCParameters.separatorChar);
            header.Append("MacrophyteMass").Append(MCParameters.separatorChar);
            header.Append("EpiphyteMass").Append(MCParameters.separatorChar);
            header.Append("InitialWaterT").Append(MCParameters.separatorChar);
            //fixed number of sediment classes
            header.Append("InitialSuspendedCoarseSand").Append(MCParameters.separatorChar);
            header.Append("InitialSuspendedMediumSand").Append(MCParameters.separatorChar);
            header.Append("InitialSuspendedFineSand").Append(MCParameters.separatorChar);
            header.Append("InitialSuspendedSilt").Append(MCParameters.separatorChar);
            header.Append("InitialSuspendedClay").Append(MCParameters.separatorChar);
            header.Append("MinimumWaterT").Append(MCParameters.separatorChar);
            header.Append("SuspendedSedimentPSaturationThreshold").Append(MCParameters.separatorChar);
            header.Append("BedSedimentPSaturationT").Append(MCParameters.separatorChar);
            header.Append("WaterTLagFactor").Append(MCParameters.separatorChar);
            header.Append("NumberOfReaches").Append(MCParameters.separatorChar);
            for (i=0; i<numberOfReaches; i++)
            {
                header.Append("reachID(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("reachName(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("SubcatchmentArea(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                for (j = 0; j < landUseClasses; j++)
                        header.Append("Land(").Append(i.ToString()).Append("_").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("BaseFlowIndex(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ThresholdSoilZoneFlow(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("RainfallExcessProportion(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("FlowErosionScalingFactor(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("FlowErosionDirectRunoffThreshold(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("FlowErosionNonLinearCoefficient(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("TransportCapacityScalingFactor(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("TransportCapacityDirectRunoffThreshold(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("TransportCapacityNonLinearCoefficieint(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("GroundwaterSustainableFlow(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("GroundwaterTimeConstant(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("MaxGroundwaterRetentionVolume(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("GroundwaterProportion(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("GroundwaterInitialFlow(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("MeanSlope(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("AquiferMatrixSorptionCoefficient(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("AquiferEquilibriumP(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("AquiferBulkDensity(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InitialGroundwaterTDP(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InitialGroundwaterInactiveP(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InactivePSaturationThreshold(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("OrganicPDryDeposition(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("OrganicPWetDeposition(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InorganicPDryDeposition(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InorganicPWetDeposition(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Length(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Width(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Slope(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Flow_a(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Flow_b(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("CoarseSandEffluentConcentration(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("MediumSandEffluentConcentration(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("FineSandEffluentConcentration(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("SiltEffluentConcentration(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ClayEffluentConcentration(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("EffluentFlowRate(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("AbstractionFlowRate(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("a7(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("a8(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("a9(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("a10(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Latitude(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Longitude(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("MacrophyteTDependency(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Epiphyte temperature dependency(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Porosity(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("MacrophyteGrowthRate(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("MacrophyteDeathRate(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("EpiphyteGrowthRate(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("EpiphyteDeathRate(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ProportionOfPInEpiphytes(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("HalfSaturationOfPForEpihpyteGrowth(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("PEexchange(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("HalfSaturationOfPForMacrophyteGrowth(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("MacrophyteSelfShadingConstant(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ProportionOfPInMacrophytes(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("WaterColumnAdsorptionCoefficient(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("StreamBedAdsorptionCoefficient(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("WaterColumnEquilibriumP(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("StreamBedEquilibriumP(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("WaterColumnFreundlichIsothermConstant(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("StreamBedFreundlichIsothermConstant(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("TDPEffluentConcentration(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("PPEffluentConcentration(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("BedDepth(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InitialCoarseSand(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InitialMediumSand(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InitialFineSand(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InitialSilt(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InitialClay(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("EffluentSwitch(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
            }
        }
    }

    class INCA_PEcoParameterArrayHeader : ParameterArrayHeader
    {
        public INCA_PEcoParameterArrayHeader(int landUseClasses, int numberOfReaches)
        {
            int i, j;
            header = new StringBuilder();
            header.Append("ID").Append(MCParameters.separatorChar);
            header.Append("ParameterSetName").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("LandCoverAbbreviation(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);           
            for (j = 0; j < landUseClasses; j++) header.Append("LandCoverName(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                   
            for (j = 0; j < landUseClasses; j++) header.Append("DirectRunoffInitialFlow(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                 //01,01
            for (j = 0; j < landUseClasses; j++) header.Append("SoilWaterFlowInitialFlow(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                //02,06
            for (j = 0; j < landUseClasses; j++) header.Append("LandPhaseInitialSedimentStore(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);           //03,09
            for (j = 0; j < landUseClasses; j++) header.Append("SoilWaterInitialTDP(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                     //04,07
            for (j = 0; j < landUseClasses; j++) header.Append("SoilInitialInactiveP(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                    //05,03
            for (j = 0; j < landUseClasses; j++) header.Append("SoilInitialLabileP(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                      //06,04
            for (j = 0; j < landUseClasses; j++) header.Append("DirectRunoffInitialTDP(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                  //07,02
            for (j = 0; j < landUseClasses; j++) header.Append("SorptionScalingFactor(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                   //08,34
            for (j = 0; j < landUseClasses; j++) header.Append("SoilwaterInitialBOD(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                     //09,08
            header.Append("startDate").Append(MCParameters.separatorChar);
            header.Append("numberOfDays").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SMDMax(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                                  //12,15
            for (j = 0; j < landUseClasses; j++) header.Append("SoilWaterSustainableFlow(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                //13,16
            for (j = 0; j < landUseClasses; j++) header.Append("VegetationIndex(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                         //14,19
            for (j = 0; j < landUseClasses; j++) header.Append("CoarseSandPct(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                           //15,24
            for (j = 0; j < landUseClasses; j++) header.Append("MediumSandPct(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                           //16,23
            for (j = 0; j < landUseClasses; j++) header.Append("FineSandPct(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                             //17,22
            for (j = 0; j < landUseClasses; j++) header.Append("SiltPct(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                                 //18,21
            for (j = 0; j < landUseClasses; j++) header.Append("ClayPct(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                                 //19,20
            for (j = 0; j < landUseClasses; j++) header.Append("MaximumInfiltrationRate(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                 //20,25
            for (j = 0; j < landUseClasses; j++) header.Append("SplashDetachmentSoilErodibilityParameter(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);//21,26
            for (j = 0; j < landUseClasses; j++) header.Append("FlowErosionSoilErodibilityParameter(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);     //22,27
            for (j = 0; j < landUseClasses; j++) header.Append("SplashDetachment_a(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                      //23,28
            for (j = 0; j < landUseClasses; j++) header.Append("PlantGrowthStartDay(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                     //24,38
            for (j = 0; j < landUseClasses; j++) header.Append("PlantGrowthPeriod(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                       //25,39
            for (j = 0; j < landUseClasses; j++) header.Append("InitialSnowDepth(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                        //26,58
            for (j = 0; j < landUseClasses; j++) header.Append("DegreeDaySnowmelt(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                       //27,59
            for (j = 0; j < landUseClasses; j++) header.Append("SWE(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                                     //28,60
            for (j = 0; j < landUseClasses; j++) header.Append("MaximumTDiff(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                            //29,63
            for (j = 0; j < landUseClasses; j++) header.Append("FertiliserAdditionStartDay(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);              //30,36
            for (j = 0; j < landUseClasses; j++) header.Append("FertiliserAdditionPeriod(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                //31,37
            for (j = 0; j < landUseClasses; j++) header.Append("PlantResidueInputs(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                      //32,45
            for (j = 0; j < landUseClasses; j++) header.Append("SolidManureAndSlurryInputs(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);              //33,46
            for (j = 0; j < landUseClasses; j++) header.Append("SolidFertiliserInputs(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                   //34,47
            for (j = 0; j < landUseClasses; j++) header.Append("LiquidManureAndSlurry(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                   //35,48
            for (j = 0; j < landUseClasses; j++) header.Append("LiquidFertiliser(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                        //36,49
            for (j = 0; j < landUseClasses; j++) header.Append("SnowDepthSoilTFactor(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                    //37,55
            for (j = 0; j < landUseClasses; j++) header.Append("ResponseToTChange(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                       //38,56
            for (j = 0; j < landUseClasses; j++) header.Append("BaseTResponse(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                           //39,57
            for (j = 0; j < landUseClasses; j++) header.Append("ThermalConductivityOfSoil(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);               //40,53
            for (j = 0; j < landUseClasses; j++) header.Append("SpecificHeatCapacityDueToFreezeThaw(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);     //41,54
            for (j = 0; j < landUseClasses; j++) header.Append("GrowthCurveOffset(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                       //42,40
            for (j = 0; j < landUseClasses; j++) header.Append("GrowthCurveAmplitude(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                    //43,41
            for (j = 0; j < landUseClasses; j++) header.Append("PlantPUptake(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                            //44,42
            for (j = 0; j < landUseClasses; j++) header.Append("Immobilisation(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                          //45,61
            for (j = 0; j < landUseClasses; j++) header.Append("TotalMaximumPUptake(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                     //46,43
            for (j = 0; j < landUseClasses; j++) header.Append("ImmobilisationTThreshold(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                //47,17
            for (j = 0; j < landUseClasses; j++) header.Append("FreundlichIsothermConstant(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);              //48,31
            for (j = 0; j < landUseClasses; j++) header.Append("WeatheringFactor(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                        //49,32
            for (j = 0; j < landUseClasses; j++) header.Append("SorptionCoefficientForSoilMatrix(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);        //50,33
            for (j = 0; j < landUseClasses; j++) header.Append("SoilInitialT(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                            //51,05
            for (j = 0; j < landUseClasses; j++) header.Append("SoilDepth(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                               //52,29
            for (j = 0; j < landUseClasses; j++) header.Append("SoilBulkDensity(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                         //53,30
            for (j = 0; j < landUseClasses; j++) header.Append("LabilePSaturationThreshold(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);              //54,18
            for (j = 0; j < landUseClasses; j++) header.Append("DailyMaximumPUptake(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                     //55,44
            for (j = 0; j < landUseClasses; j++) header.Append("EnrichmentRatio(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                         //56,35
            for (j = 0; j < landUseClasses; j++) header.Append("BODDecayRate(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                            //57,62
            for (j = 0; j < landUseClasses; j++) header.Append("BODForPlantResidue(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                      //58,50
            for (j = 0; j < landUseClasses; j++) header.Append("BODForSolidManureAndSlurry(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);              //59,51
            for (j = 0; j < landUseClasses; j++) header.Append("BODForAnialFeces(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                        //60,52
            for (j = 0; j < landUseClasses; j++) header.Append("DirectRunoffDO(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                          //61,13
            for (j = 0; j < landUseClasses; j++) header.Append("SoilwaterDO(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                             //62,14
            for (j = 0; j < landUseClasses; j++) header.Append("DirectRunoffTimeConstant(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                //63,11
            for (j = 0; j < landUseClasses; j++) header.Append("SoilwaterTimeConstant(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);                   //64,12
            for (j = 0; j < landUseClasses; j++) header.Append("SoilWaterRetentionVolumeConstant(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);        //65,10  
            header.Append("Flow").Append(MCParameters.separatorChar);                                                                                                           //66,78
            header.Append("WaterColumnTDP").Append(MCParameters.separatorChar);                                                                                                 //67,72
            header.Append("StreamBedTDP").Append(MCParameters.separatorChar);                                                                                                   //68,73
            header.Append("WaterColumnPP").Append(MCParameters.separatorChar);                                                                                                  //69,74
            header.Append("StreamBedPP").Append(MCParameters.separatorChar);                                                                                                    //70,75
            header.Append("MacrophyteMass").Append(MCParameters.separatorChar);                                                                                                 //71,76
            header.Append("EpiphyteMass").Append(MCParameters.separatorChar);                                                                                                   //72,77
            header.Append("InitialWaterT").Append(MCParameters.separatorChar);                                                                                                  //73,79
            //fixed number of sediment classes
            header.Append("InitialSuspendedCoarseSand").Append(MCParameters.separatorChar);                                                                                     //74,67
            header.Append("InitialSuspendedMediumSand").Append(MCParameters.separatorChar);                                                                                     //75,68
            header.Append("InitialSuspendedFineSand").Append(MCParameters.separatorChar);                                                                                       //76,69
            header.Append("InitialSuspendedSilt").Append(MCParameters.separatorChar);                                                                                           //77,70
            header.Append("InitialSuspendedClay").Append(MCParameters.separatorChar);                                                                                           //78,71
            header.Append("InitialLivePhytoplankton").Append(MCParameters.separatorChar);                                                                                       //79,64
            header.Append("InitialDO").Append(MCParameters.separatorChar);                                                                                                      //80,65
            header.Append("InitialBOD").Append(MCParameters.separatorChar);                                                                                                     //81,66
            header.Append("MinimumWaterT").Append(MCParameters.separatorChar);                                                                                                  //82,80
            header.Append("SuspendedSedimentPSaturationThreshold").Append(MCParameters.separatorChar);                                                                          //83,81        
            header.Append("BedSedimentPSaturationT").Append(MCParameters.separatorChar);                                                                                        //84,82                                                                        
            header.Append("WaterTLagFactor").Append(MCParameters.separatorChar);                                                                                                //85,83
            header.Append("NumberOfReaches").Append(MCParameters.separatorChar);                                                                                                //86,NA
            for (i = 0; i < numberOfReaches; i++)
            {
                header.Append("reachID(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("reachName(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("SubcatchmentArea(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);                                                         //,153
                for (j = 0; j < landUseClasses; j++)
                    header.Append("Land(").Append(i.ToString()).Append("_").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("BaseFlowIndex(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);                                                            //,154
                header.Append("ThresholdSoilZoneFlow(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);                                                    //,155
                header.Append("RainfallExcessProportion(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);                                                 //,156
                header.Append("FlowErosionScalingFactor(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);                                                 //,157
                header.Append("FlowErosionDirectRunoffThreshold(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);                                         //,158
                header.Append("FlowErosionNonLinearCoefficient(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);                                          //,159
                header.Append("TransportCapacityScalingFactor(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("TransportCapacityDirectRunoffThreshold(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("TransportCapacityNonLinearCoefficieint(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("GroundwaterSustainableFlow(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("GroundwaterTimeConstant(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("MaxGroundwaterRetentionVolume(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("GroundwaterProportion(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("GroundwaterInitialFlow(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("MeanSlope(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("AquiferMatrixSorptionCoefficient(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("AquiferEquilibriumP(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("AquiferBulkDensity(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InitialGroundwaterTDP(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InitialGroundwaterInactiveP(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InactivePSaturationThreshold(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("OrganicPDryDeposition(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("OrganicPWetDeposition(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InorganicPDryDeposition(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InorganicPWetDeposition(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Length(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Width(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Slope(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Flow_a(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Flow_b(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("CoarseSandEffluentConcentration(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("MediumSandEffluentConcentration(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("FineSandEffluentConcentration(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("SiltEffluentConcentration(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ClayEffluentConcentration(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("EffluentFlowRate(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("AbstractionFlowRate(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("a7(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("a8(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("a9(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("a10(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Latitude(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Longitude(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("MacrophyteTDependency(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Epiphyte temperature dependency(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Porosity(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("MacrophyteGrowthRate(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("MacrophyteDeathRate(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("EpiphyteGrowthRate(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("EpiphyteDeathRate(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ProportionOfPInEpiphytes(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("HalfSaturationOfPForEpihpyteGrowth(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("PEexchange(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("HalfSaturationOfPForMacrophyteGrowth(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("MacrophyteSelfShadingConstant(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ProportionOfPInMacrophytes(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("WaterColumnAdsorptionCoefficient(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("StreamBedAdsorptionCoefficient(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("WaterColumnEquilibriumP(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("StreamBedEquilibriumP(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("WaterColumnFreundlichIsothermConstant(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("StreamBedFreundlichIsothermConstant(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("TDPEffluentConcentration(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("PPEffluentConcentration(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("BedDepth(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InitialCoarseSand(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InitialMediumSand(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InitialFineSand(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InitialSilt(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InitialClay(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("EffluentSwitch(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
            }
        }
    }
    class INCA_ToxParameterArrayHeader : ParameterArrayList
    {
        public INCA_ToxParameterArrayHeader(int landUseClasses, int numberOfReaches, int numberOfGrainSizes, int numberOfContaminants)
        {
            int h, i, j, k;
            header = new StringBuilder();
            header.Append("ParameterSetName").Append(MCParameters.separatorChar);
            header.Append("NumberOfGrainSsizeClasses").Append(MCParameters.separatorChar);
            for (h = 0; h < numberOfGrainSizes; h++) header.Append("GrainSizeClassName(").Append(h.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (h = 0; h < numberOfGrainSizes; h++) header.Append("MaximumDiameter_um(").Append(h.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (h = 0; h < numberOfGrainSizes; h++) header.Append("MinimumDiameter_um(").Append(h.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (h = 0; h < numberOfGrainSizes; h++) header.Append("Density(").Append(h.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (h = 0; h < numberOfGrainSizes; h++) header.Append("InmanParameter1(").Append(h.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (h = 0; h < numberOfGrainSizes; h++) header.Append("InmanParameters(").Append(h.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("LandCoverAbbreviation(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("LandCoverName(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            header.Append("NumberOfContaminants").Append(MCParameters.separatorChar);            
            for (j = 0; j < landUseClasses; j++) header.Append("DirectRunoffInitialFlow(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerInitialFlow(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerInitialFlow(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DirectRunoffInitialPDC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerInitialSOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerInitialDOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerInitialDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerInitialSOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerInitialDOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerInitialDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SedimentStoreInitialCondition(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            header.Append("startDate").Append(MCParameters.separatorChar);
            header.Append("numberOfDays").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SMDMaxForChemistry(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerMinimumFlow(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("VegetationIndex(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MaximumInfiltrationRate(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilErodibilityForSplashDetachment(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilErodibilityForFlowErosion(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SplashDetachmant_a_Parameter(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("PlantGrowthStartDay(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("PlantGrowthPeriod(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("InitialSnowDepth(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DegreeDayMeltFactor(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SnowWaterEquivalent(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerPDCToSOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerPDCToDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerPDCToDOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerSOCToDOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerSOCToDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerDOCToSOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerDOCToDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerSOCToDOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerSOCToDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerDOCToSOC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerDOCToDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerCO2MassTransferVelocity(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DailyPDCInputToOrganicLayer(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("LitterfallStartDay(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("LitterfallDuration(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("LitterfallAmount(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MaximumTDifference(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SnowDepthFactor(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("COUP_10DegreeResponse(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("COUP_BaseT(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilTThermalConductivity(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilTSpecificHeatCapacity(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MinimumMineralLayerFlow(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerMaxDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerMaxDIC(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerCO2MassTransferVelocity(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerReturnTime(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerEnzymaticLatchRate(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerCriticalSMD(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerReturnTime(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerEnzymaticLatchRate(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerCriticalSMD(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerB1(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerB2(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerB1(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerB2(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("Delta(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MaxSMDForOLRetentionVolume(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MaxSMDForMLRetentionVolume(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("InitialSoilT(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("EasilyAccessibleFraction(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrgaqnicLayerPorosity(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerPorosity(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("TBLPathLength(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("CBLPathLength(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("WindSpeedTDCoefficient(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("EffectiveBioturbationCoefficient(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerDepth(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerDepth(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("GaseousExchangeFraction(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (h = 0; h < numberOfGrainSizes; h++)
            {
                for (j = 0; j < landUseClasses; j++) header.Append("GrainSizePct(").Append(h.ToString()).Append(".").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            }

            for (j = 0; j < landUseClasses; j++) header.Append("DirectRunoffTimeConstant(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerTimeConstant(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerTimeConstant(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("OrganicLayerTotalToAvailableWaterRatio(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralLayerTotalToAvailableWaterRatio(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            header.Append("NumberOfContaminants").Append(MCParameters.separatorChar);
            for (i = 0; i < numberOfContaminants; i++)
            {
                header.Append("ContaminantName(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("SWHenrysLawConstant(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("SWEnthalpyAW(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("SWOctanolWater(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("SWEnthalpyOW(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("SWEasyToPotentialTimeConstant(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("MolarVolume(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("MolarMass(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                for (h = 0; h < numberOfGrainSizes; h++)
                {
                    header.Append("GrainSizekSOCFactor(").Append(i.ToString()).Append(".").Append(h.ToString()).Append(")").Append(MCParameters.separatorChar);
                }
                for (j = 0; j < landUseClasses; j++)
                {
                    header.Append("UpperLayerInitialBulkConcentration(").Append(i.ToString()).Append(".").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
                    header.Append("LowerLayerInitialBulkConcentration(").Append(i.ToString()).Append(".").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
                    header.Append("AtmosphericDryParticleDeposition(").Append(i.ToString()).Append(".").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
                    header.Append("AtmosphericWetDeposition(").Append(i.ToString()).Append(".").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
                    header.Append("MassAppliedInFertilizer(").Append(i.ToString()).Append(".").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
                    header.Append("MassInLitterfall(").Append(i.ToString()).Append(".").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
                    header.Append("TimeToEquilibrate(").Append(i.ToString()).Append(".").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
                }
            }
            for (int r = 0; r < numberOfContaminants; r++)
            {
                for (int c = 0; c < numberOfContaminants; c++)
                {
                    header.Append("FormationMatrix(").Append(r.ToString()).Append(".").Append(c.ToString()).Append(")").Append(MCParameters.separatorChar);
                }
            }
            header.Append("InitialFlow").Append(MCParameters.separatorChar);
            header.Append("OpenWaterPDC").Append(MCParameters.separatorChar);
            header.Append("OpenWaterDOC").Append(MCParameters.separatorChar);
            header.Append("OpenWaterDIC").Append(MCParameters.separatorChar);
            header.Append("StreambedPDC").Append(MCParameters.separatorChar);
            header.Append("StreambedDOC").Append(MCParameters.separatorChar);
            header.Append("StreambedDIC").Append(MCParameters.separatorChar);
            header.Append("InitialWaterTemperature").Append(MCParameters.separatorChar);
            for (h = 0; h < numberOfGrainSizes; h++) header.Append("InitialSSConcentration(").Append(h.ToString()).Append(")").Append(MCParameters.separatorChar);
            header.Append("MinimumWaterTemperature").Append(MCParameters.separatorChar);
            header.Append("WaterTemperatureLagFactor").Append(MCParameters.separatorChar);
            for (i = 0; i < numberOfContaminants; i++)
            {
                header.Append("WaterColumnInitualBulkConcentration(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("UpperSedimentInitialBulkConcentration(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("LowerSedimentInitialBulkConcentration(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InWaterDegradationHalfLife(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ShelteringFactor(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("SolidSedimentDegradationHalfLife(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("FreelyDissolvedSedimentDegradationHalfLife(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("OpenWaterTimeToEquilibrate(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
            }
            header.Append("NumberOfReaches").Append(MCParameters.separatorChar);
            for (k = 0; k < numberOfReaches; k++)
            {
                header.Append("ReachName(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ReachID(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("SubCatchmentArea(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                for (j = 0; j < landUseClasses; j++) header.Append("LandCoverPct(").Append(k.ToString()).Append(".").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("DeltaMG(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ThresholdSoilZoneFlow(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("RainfallExcessProportion(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("FlowErosionScalingFactor(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("FlowErosionDirectRunoffThreshold(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("FlowErosionNonLinearCoefficient(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("TransportCapacityScalingFactor(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("TransportCapacityDirectRunoffThreshold(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("TransportCapacityNonLinearCoefficient(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("GroundwaterSustainableFlow(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("GroundwaterTimeConstant(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("MaximumGroundwaterRetentionVolume(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("GroundwaterProportion(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("GroundwaterInitialFlow(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("MeanSlope(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("CO2SaturationConcentration(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("AtmosphericCO2PartialPressure(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("HenrysLawConstantForCO2(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("SO4DryDeposition(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("SO4WetDeposition(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("GroundwaterInitialSOC(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("GroundwaterInitialDOC(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("GroundwaterInitialDIC(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("DeltaOM(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("DeltaOG(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                for (i = 0; i < numberOfContaminants; i++)
                {
                    header.Append("OLEADegradationHalfLife(").Append(k.ToString()).Append(".").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                    header.Append("OLPADegradationHalfLife(").Append(k.ToString()).Append(".").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                    header.Append("MLEADegradationHalfLife(").Append(k.ToString()).Append(".").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                    header.Append("MLPADegradationHalfLife(").Append(k.ToString()).Append(".").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                    header.Append("AtmosphericConcentration(").Append(k.ToString()).Append(".").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                }
                header.Append("ReachLength(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ReachWidth(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ReachSlope(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Flow_a(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Flow_b(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("a7(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("a8(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("a9(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("a10(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Latitude(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Longitude(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ReachArea(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("OpenWaterPDCSettlingVelocity(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("OpenWaterDocToDICSelfShadingFactor(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("OpenWaterDOCToDICRadiationMultiplier(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("OpenWaterMicrobialDOCToDIC(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("OpenWaterPDCToDOC(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("OpenWaterDOCToPDC(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("OpenWaterDICMassTransfer(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("OpenWaterDICToPDC(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("BedPorosity(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("BedDepth(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("WaterColumnToPoreWaterCExchange(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("StreamBedDOCToPDC(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("StreamBedPDCToDOC(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("StreamBedPDCMineralization(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("StreamBedDOCMineralization(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("AbstractionFlowRate(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("EffluentFlowRate(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("EffluentDOCConcentration(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ReachLitterfallRate(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("EffluentDICConcentraiton(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("EffluentOnOff(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("isWaterfall(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("hE(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("dS(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("WaterfallHeight(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("dp(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("SedimentEffectiveBioturbationCoefficient(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("BioturbationSizeThreshold(").Append(k.ToString()).Append(")").Append(MCParameters.separatorChar);
                for (h = 0; h < numberOfGrainSizes; h++) header.Append("GrainSizeEffluentConcentraiton(").Append(k.ToString()).Append(".").Append(h.ToString()).Append(")").Append(MCParameters.separatorChar);
                for (h = 0; h < numberOfGrainSizes; h++) header.Append("InitialBedSedimentGrainSizeClass(").Append(k.ToString()).Append(".").Append(h.ToString()).Append(")").Append(MCParameters.separatorChar);
                for (h = 0; h < numberOfGrainSizes; h++) header.Append("GrainSizeOCFraction(").Append(k.ToString()).Append(".").Append(h.ToString()).Append(")").Append(MCParameters.separatorChar);
                for (i = 0; i < numberOfContaminants; i++)
                {
                    header.Append("FreelyDissolvedEffluentConcentration(").Append(k.ToString()).Append(".").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                    header.Append("DOCAssociatedEffluentConcentration(").Append(k.ToString()).Append(".").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                    header.Append("SOCAssociatedEffluentConcentration(").Append(k.ToString()).Append(".").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                }
            }
        }
    }

    class Branching_INCA_N_With_PERSiSTParameterArrayHeader : ParameterArrayHeader
    {
        public Branching_INCA_N_With_PERSiSTParameterArrayHeader(int landUseClasses, int numberOfReaches, int numberOfBoxes)
        {
            int i, j, k, l, m;
            header = new StringBuilder();
            header.Append("ID").Append(MCParameters.separatorChar);
            header.Append("ParameterSetName").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("LandCoverAbbreviation(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("LandCoverName(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);          
            for (j = 0; j < landUseClasses; j++) header.Append("DirectRunoffInitialFlow(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilWaterInitialFlow(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DirectRunoffInitialNitrate(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilWaterInitialNitrate(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DirectRunoffInitialAmmonium(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilWaterInitialAmmonium(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            header.Append("startDate").Append(MCParameters.separatorChar);
            header.Append("numberOfDays").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilWaterDenitrification(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("NitrogenFixation(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("PlantNitrateUptakeRate(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MaximumNitrogenUptakeRate(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("NitrateAdditionRate(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("AmmoniumNitrificationRate(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("AmmoniumMineralisationRate(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("AmmoniumImmobilisationRate(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("AmmoniumAdditionRate(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("PlantAmmoniumUptakeRate(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("PlantGrowthStartDay(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("PlantGrowthPeriod(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("FertiliserAdditionStartDay(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("FertiliserAdditionPeriod(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SMDMax(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MaximumTDifference(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DenitrificationTThreshold(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("NitrificationTThreshold(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralisationTThreshold(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("ImmobilisationTThreshold(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilWaterSustainableFlow(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DirectRunoffSustainableFlow(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DenitrificationTResponse(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DenitrificationBaseT(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("InitialSnowDepth(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DegreeDaySnowmelt(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SnowWaterEquivalentFactor(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SnowDepthSoilTFactor(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilThermalConductivity(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("FreezeThawSpecificHeatCapacity(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("GrowthCurveOffset(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("GrowthCurveAmplitude(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("NFixationTResponse(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("NFixationBaseT(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("NitrificationTResponse(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("NitrificationBaseT(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralisationTResponse(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MineralisationBaseT(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("ImmobilisationTResponse(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("ImmobilisationBaseT(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("NitrateUptakeTResponse(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("NitrateUptakeBaseT(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("AmmoniumUptakeTResponse(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("AmmoniunUptakeBaseT(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("ZeroRateDepth(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("MaxRateDepth(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DirectTimeConstant(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SoilTimeConstant(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("TotalToAvailableRatio(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            header.Append("InitialFlow").Append(MCParameters.separatorChar);
            header.Append("InitialNitrate").Append(MCParameters.separatorChar);
            header.Append("InitialAmmonium").Append(MCParameters.separatorChar);
            header.Append("MinimumWaterT").Append(MCParameters.separatorChar);
            header.Append("NumberOfReaches").Append(MCParameters.separatorChar);


            for (i = 0; i < numberOfReaches; i++)
            {
                header.Append("ReachNumber(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ReachName(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ReachArea(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                for (k = 0; k < landUseClasses; k++)
                {
                    header.Append("LandUse(").Append(i.ToString()).Append(")(").Append(k.ToString()).Append(MCParameters.separatorChar);
                }
                header.Append("BaseFlowIndex(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ThresholdSoilZoneFlow(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("RainfallExcessProportion(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("MaximumInfiltrationRate(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("GroundwaterDenitrificationRate(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("MaxGroundwaterEffectiveDepth(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ProportionOfFilledPoreSpace(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("GroundwaterResidenceTime(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InitialGroundwaterFlow(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InitialGroundwaterNitrate(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InitialGroundwaterAmmonium(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("GroundwaterSustainableFlow(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("NitrateDryDeposition(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("NitrateWetDeposition(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("AmmoniumDryDeposition(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("AmmoniumWetDeposition(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ReachLength(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Flow_a(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Flow_b(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InstreamNitrification(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("InstreamDenitrification(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("EffluentNitrateConcentration(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("EffluentAmmoniumConcentration(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("EffluentFlow(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("AbstractionRate(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("HasAbstractionOrEffluent(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Latitude(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("Longitude(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("PrecipitationAsSnow(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ReachSnowMultiplier(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("ReachRainMultiplier(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("StreamWidth(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
            }
            header.Append("UnknownNumber").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SnowMultiplier(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("SnowMeltT(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DegreeDayMeltFactor(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("RainMultiplier(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("InitialSnowDepth(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("GrowingDegreeThreshold(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (j = 0; j < landUseClasses; j++) header.Append("DegreeDayEvapotranspiration(").Append(j.ToString()).Append(")").Append(MCParameters.separatorChar);
            for (i = 0; i < numberOfReaches; i++)
            {
                header.Append("SoilBoxName(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                for (k = 0; k < landUseClasses; k++) header.Append("InitialWaterDepth(").Append(i.ToString()).Append(")(").Append(k.ToString()).Append(MCParameters.separatorChar);
                for (k = 0; k < landUseClasses; k++) header.Append("RelativeAreaIndex(").Append(i.ToString()).Append(")(").Append(k.ToString()).Append(MCParameters.separatorChar);
                for (k = 0; k < landUseClasses; k++) header.Append("InfiltrationCapacity(").Append(i.ToString()).Append(")(").Append(k.ToString()).Append(MCParameters.separatorChar);
                for (k = 0; k < landUseClasses; k++) header.Append("RetainedWaterDepth(").Append(i.ToString()).Append(")(").Append(k.ToString()).Append(MCParameters.separatorChar);
                for (k = 0; k < landUseClasses; k++) header.Append("DroughtRunofFraction(").Append(i.ToString()).Append(")(").Append(k.ToString()).Append(MCParameters.separatorChar);
                for (k = 0; k < landUseClasses; k++) header.Append("ResidenceTime(").Append(i.ToString()).Append(")(").Append(k.ToString()).Append(MCParameters.separatorChar);
                for (k = 0; k < landUseClasses; k++) header.Append("ETAdjustment(").Append(i.ToString()).Append(")(").Append(k.ToString()).Append(MCParameters.separatorChar);
                for (k = 0; k < landUseClasses; k++) header.Append("RelativeETIndex(").Append(i.ToString()).Append(")(").Append(k.ToString()).Append(MCParameters.separatorChar);
                for (k = 0; k < landUseClasses; k++) header.Append("MaxCapacity(").Append(i.ToString()).Append(")(").Append(k.ToString()).Append(MCParameters.separatorChar);
                for (k = 0; k < landUseClasses; k++) header.Append("InundationThreshold(").Append(i.ToString()).Append(")(").Append(k.ToString()).Append(MCParameters.separatorChar);
                for (k = 0; k < landUseClasses; k++) header.Append("Porosity(").Append(i.ToString()).Append(")(").Append(k.ToString()).Append(MCParameters.separatorChar);
                header.Append("IsQuickBox(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("AllowBidirectionalFlow(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                header.Append("UseForSMDCalculation(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
           }
            for (j = 0; j < landUseClasses; j++)
            {
                header.Append("LandCoverNumber(").Append(i.ToString()).Append(")").Append(MCParameters.separatorChar);
                for (l = 0; l < numberOfBoxes; l++)
                {
                    for (m = 0; m < numberOfBoxes; m++)
                    {
                        header.Append("SM(").Append(j.ToString()).Append(")(").Append(l.ToString()).Append(")(").Append(m.ToString()).Append(")").Append(MCParameters.separatorChar);
                    }
                }
            }
        }
    }

    class INCA_HgParameterArrayHeader: ParameterArrayHeader
    {
        public INCA_HgParameterArrayHeader(int landuseClasses, int reaches)
        {
            header = new StringBuilder();
        }
    }
}
