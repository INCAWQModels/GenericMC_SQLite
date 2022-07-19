using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace MC
{
    class ParameterArrayList
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

    class PERSiSTParameterArrayList : ParameterArrayList
    {
        public PERSiSTParameterArrayList(int landUseClasses, int numberOfReaches, int numberOfBoxes)
        {
            int i, j, k, l, m;
            header = new StringBuilder();
            m = 0;
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("TimeSteps").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StartDate").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilBoxCount").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BaselineSMD").Append("\n");

            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Name(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SnowMultiplier(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SnowMelt(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DegreeDayMeltFactor(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("RainMultiplier(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSnowDepth(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GrowingDegreeThreshold(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DegreeDayEvapotranspiration(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Interception(").Append(j.ToString()).Append(")").Append("\n");

            for (k = 0; k < numberOfBoxes; k++)
            {
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Name(").Append(k.ToString()).Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialWaterDepth(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("RelativeAreaIndex(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Infiltration(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("RetainedWaterDepth(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DroughtRunoffFraction(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ResidenceTime(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EvapotranspirationAdjustment(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("RelativeEvapotranspirationIndex(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FieldCapacity(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InundationThreshold(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Porosity(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InfiltrationOffset(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("IsQuickBox(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AllowInfiltration(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AllowInundation(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("UseSMD(").Append(k.ToString()).Append(")").Append("\n");
            }
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachCount").Append("\n");
            for (i = 0; i < numberOfReaches; i++)
            {
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachName(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("CatchmentArea(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialReachFlow(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachLength(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Width(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("a(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("b(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PrecipAsSnow(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachSnowMultiplier(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachRainMultiplier(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Abstraction(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Effluent(").Append(i.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandscapeUnitPct(").Append(j.ToString()).Append(".").Append(i.ToString()).Append(")").Append("\n");
            }
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SquareMatrix(").Append(j.ToString()).Append(")").Append("\n");
            for (k = 0; k < numberOfBoxes; k++)
            {
                for (l = 0; l < numberOfBoxes; l++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SM(").Append(k.ToString()).Append(".").Append(l.ToString()).Append("\n");
            }
        }
    }

    class PERSiST_v2ParameterArrayList : ParameterArrayList //still needs to be edited
    {
        public PERSiST_v2ParameterArrayList(int landUseClasses, int numberOfReaches, int numberOfBoxes)
        {
            int i, j, k, l, m;
            header = new StringBuilder();
            m = 0;
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SecondsPerTimeStep").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("TimeSteps").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StartDate").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StartTime").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilBoxCount").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BaselineSMD").Append("\n");

            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Name(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverSnowMultiplier(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverSnowMeltTemperature(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverDegreeDayMeltFactor(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverRainMultiplier(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverInitialSnowDepth(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverGrowingDegreeThreshold(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverCanopyInterception(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverSolarRadiationScalingFactor(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverInitialSoilTemperature(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverSoilThermalConductivity(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverSpecificHeatOfFreezeThaw(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverSnowDepthFactor(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverMinimumSoilTemperatureForInfiltration(").Append(j.ToString()).Append(")").Append("\n");

            for (k = 0; k < numberOfBoxes; k++)
            {
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Name(").Append(k.ToString()).Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BoxInitialWaterDepth(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BoxRelativeAreaIndex(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BoxInfiltration(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BoxRetainedWaterDepth(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BoxDroughtRunoffFraction(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BoxCharacteristicTimeConstant(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BoxEvapotranspirationAdjustment(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BoxRelativeEvapotranspirationIndex(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BoxFieldCapacity(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BoxInundationThreshold(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BoxPorosity(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BoxInfiltrationOffset(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("IsQuickBox(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AllowInfiltration(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AllowInundation(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("UseSMD(").Append(k.ToString()).Append(")").Append("\n");
            }
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachCount").Append("\n");
            for (i = 0; i < numberOfReaches; i++)
            {
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachName(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("CatchmentArea(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachInitialFlow(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachLength(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachWidthAtSedimentSurface(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("a(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("b(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("c(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("f(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("n(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Slope(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SubcatchmentSnowThresholdTemperature(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SubcatchmentSnowMultiplier(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SubcatchmentRainMultiplier(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Abstraction(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Effluent(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Latitude(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Longitude(").Append(i.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandscapeUnitPct(").Append(j.ToString()).Append(".").Append(i.ToString()).Append(")").Append("\n");
            }
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SquareMatrix(").Append(j.ToString()).Append(")").Append("\n");
            for (k = 0; k < numberOfBoxes; k++)
            {
                for (l = 0; l < numberOfBoxes; l++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SM(").Append(k.ToString()).Append(".").Append(l.ToString()).Append("\n");
            }
        }
    }

    class PERSiST1_6ParameterArrayList : ParameterArrayList
    {
        public PERSiST1_6ParameterArrayList(int landUseClasses, int numberOfReaches, int numberOfBoxes)
        {
            int i, j, k, l, m;
            header = new StringBuilder();
            m = 0;
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("TimeSteps").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StartDate").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilBoxCount").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BaselineSMD").Append("\n");

            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Name(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SnowMultiplier(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SnowMelt(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DegreeDayMeltFactor(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("RainMultiplier(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSnowDepth(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GrowingDegreeThreshold(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Interception(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ETOffset(").Append(j.ToString()).Append(")").Append("\n");

            for (k = 0; k < numberOfBoxes; k++)
            {
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Name(").Append(k.ToString()).Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialWaterDepth(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("RelativeAreaIndex(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Infiltration(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("RetainedWaterDepth(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DroughtRunoffFraction(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ResidenceTime(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EvapotranspirationAdjustment(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("RelativeEvapotranspirationIndex(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FieldCapacity(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InundationThreshold(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Porosity(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InfiltrationOffset(").Append(j.ToString()).Append(".").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("IsQuickBox(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AllowInfiltration(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AllowInundation(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("UseSMD(").Append(k.ToString()).Append(")").Append("\n");
            }
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachCount").Append("\n");
            for (i = 0; i < numberOfReaches; i++)
            {
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachName(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("CatchmentArea(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialReachFlow(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachLength(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Width(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("a(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("b(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("c(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("f(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("n(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Slope(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PrecipAsSnow(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachSnowMultiplier(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachRainMultiplier(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Abstraction(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Effluent(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Latitude(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Longitude(").Append(i.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandscapeUnitPct(").Append(j.ToString()).Append(".").Append(i.ToString()).Append(")").Append("\n");
            }
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SquareMatrix(").Append(j.ToString()).Append(")").Append("\n");
            for (k = 0; k < numberOfBoxes; k++)
            {
                for (l = 0; l < numberOfBoxes; l++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SM(").Append(k.ToString()).Append(".").Append(l.ToString()).Append("\n");
            }
        }
    }

    class INCA_CParameterArrayList : ParameterArrayList
    {
        public INCA_CParameterArrayList(int landUseClasses, int numberOfReaches)
        {
            int i, j, m;
            m = 0;
            header = new StringBuilder();
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ParameterSetName").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverAbbreviation(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverName(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffInitialFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerInitialFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerInitialFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffInitialPDC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerInitialSOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerInitialDOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerInitialDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerInitialSOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerInitialDOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerInitialDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffInitialDOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSoilTemperature(").Append(j.ToString()).Append(")").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("startDate").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("numberOfDays").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerPDCToSOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerPDCToDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerPDCToDOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerSOCToDOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerSOCToDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerDOCToSOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerDOCToDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerSOCToDOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerSOCToDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerDOCToSOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerDOCToDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerCO2MassTransferVelocity(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DailyPDCInputToOrganicLayer(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FertilizerAdditionStartDay(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FertilizerAdditionDuration(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FertilizerAdditionAmount(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaximumSoilMoistureDeficit(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilTDiffBnSummerAndWinterMax(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SnowDepthFactor(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("COUP_10DegreeResponse(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("COUP_BaseT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SnowPackInitialDepth(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SnowPackDegreeDayMeltFactor(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SnowPackWaterEquivalent(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilTThermalConductivity(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilTSpecificHeatCapacity(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MinimumOrganicLayerFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MinimumMineralLayerFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerMaxDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerMaxDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerCO2MassTransferVelocity(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerReturnTime(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerEnzymaticLatchRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerCriticalSMD(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerReturnTime(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerEnzymaticLatchRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerCriticalSMD(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerB1(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerB2(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerB1(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerB2(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ZeroRateDepth(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaxRateDepth(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaxSMDFraction(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffPDCToDOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerFastPoolFraction(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerPartitioningRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerFastPoolFraction(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerPartitioningRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffResidenceTime(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerResidenceTime(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerResidenceTime(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerRetentionVolume(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerRetentionVolume(").Append(j.ToString()).Append(")").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("RiverFlow").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialOpenWaterPDC").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialOpenWaterDOC").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialOpenWaterDIC").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialWaterTemperature").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MinimumWaterTemperature").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WaterTemperatureLagFactor").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("numberOfReaches").Append("\n");
            for (i = 0; i < numberOfReaches; i++)
            {
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("reachName(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("reachLength(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("averageWidth(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("latitude(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("longitude(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("flow_a(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("flow_b(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachArea(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OpenWaterPDCSettlingVelocity(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DOCToDICSelfShadingFactor(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DOCToDICRadiationMultiplier(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OpenWaterDOCToDICMicrobial(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OpenWaterPDCToDOC(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OpenWaterDOCToPDC(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OpenWaterDICMassTransferRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DICToPDCPhotosynthesis(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AbstractionRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentInputRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentConcentration(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LitterFallOntoReach(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AbstractionOnOffSwitch(").Append(i.ToString()).Append(")").Append("\n");
            }
            for (i = 0; i < numberOfReaches; i++)
            {
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("CatchmentArea(").Append(i.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++)
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Land(").Append(i.ToString()).Append("_").Append(j.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BaseFlowIndex(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ThresholdSoilZoneFlow(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("CO2SaturationConcentration(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AtmosphericPartialPressureOfCO2(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("RainfallExcessProportion(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaximumInfiltrationRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DryDeposition(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WetDeposition(").Append(i.ToString()).Append(")").Append("\n");

            }
        }
    }

    class INCA_HgParameterArrayList : ParameterArrayList
    {
        public INCA_HgParameterArrayList(int landUseClasses, int numberOfReaches)
        {
            int i, j, m;
            m = 0;
            header = new StringBuilder();
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ParameterSetName").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverAbbreviation(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverName(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("01_DirectRunoffInitialFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("02_OrganicLayerInitialFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("03_MineralLayerInitialFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("04_SoilInitialSedimentStore(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("05_DirectRunoffInitialLitterMass(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("06_InitialSnowpackHg0(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("07_InitialSnowpackHg2(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("08_InitialSnowpackMeHg(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("09_DirectRunoffInitialDissolvedHg2(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("10_DirectRunoffInitialDissolvedMeHg(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("11_DirectRunoffInitialSolidPhaseHg2(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("12_DirectRunoffInitialSolidPhaseMeHg(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("13_UpperSoilInitialDissolvedHg2(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("14_UpperSoilInitialDissolvedMeHg(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("15_UpperSoilInitialSolidPhaseHg2(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("16_UpperSoilInitialSolidPhaseMeHg(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("17_LowerSoilInitialDissolvedHg2(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("18_LowerSoilInitialDissolvedMeHg(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("19_LowerSoilInitialSolidPhaseHg2(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("20_LowerSoilInitialSolidPhaseMeHg(").Append(j.ToString()).Append(")").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("startDate").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("numberOfDays").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("21_RetentionVolumeMaxSMD(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("22_MinimumUpperSoilFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("23_VegetationIndex(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("24_GrainSizeCoarseSandProportion(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("25_GrainSizeMediumSandProportion(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("26_GrainSizeFineSandProportion(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("27_GrainSizeSiltProportion(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("28_GrainSizeClayProportion(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("29_MaximumInfiltrationRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("30_ErosionSplashE(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("31_ErosionFlowE(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("32_ErosionSplashA(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("33_GrowthSeasonStartDay(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("34_GrowthSeasonGrowthPeriod(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("35_SnowpackInitialDepth(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("36_SnowpackDegreeDayMelt(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("37_SnowpackWaterEquivalentFactor(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("38_SoilTemperatureDifferenceBetweenMaximums(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("39_SoilTemperatureSnowDepthFactor(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("40_TemperatureResponse10DegreeChange(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("41_TemperatureResponseBaseTemperature(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("42_SoilThermanConductivity(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("43_SoilSpecificHeatCapacityDueToFreezeThaw(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("44_UpperSoilReductionRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("45_UpperSoilMethylationRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("46_UpperSoilDemethylationRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("47_LowerSoilReductionRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("48_LowerSoilMethylationRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("49_LowerSoilDemethylationRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("50_UpperSoilMeHgDesorption(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("51_LowerSoilMeHgDesorption(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("52_SnowpackHg2ReductionRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("53_SnowpackHg0VolatilizationRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("54_UpperSoilOrganicCarbonSorptionRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("55_UpperSoilOrganicCarbonDesorptionRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("56_LowerSoilOrganicCarbonSorptionRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("57_LowerSoilOrganicCarbonDesorptionRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("58_UpperSoilPDCIncorporationIntoSOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("59_LitterLayerHg2ToUpperSoil(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("60_LitterLayerMeHgToUpperSoil(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("61_LitterLayerOrganicCarbonMineralization(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("62_MinimumLitterLayerMassForErosion(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("63_HgMaxRateDepth(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("64_TemperatureDependentDemethylationMultiplier(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("65_TemperatureDependentDemethylationOffset(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("66_OCTransformationMaxRateDepth(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("67_TemperatureDependentCarbonTransformationMultiplier(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("68_TemperatureDependentCarbonTransformationOffset(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("69_ThroughfallHg2Input(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("70_ThroughfallMeHgInput(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("71_LitterfallHg2Input(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("72_LitterfallMeHgInput(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("73_LitterfallMass(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("74_MinimumLowerSoilFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("75_OCTransformationZeroRateDepth(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("76_OCTransformationMaximumSMDFraction(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("77_HgZeroRateDepth(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("78_HgMaximumSMDFraction(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("79_InitialSoilTemperature(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("80_OrganicLayerFastPoolFraction(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("81_OrganicLayerRateConstant(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("82_MineralLayerFastPoolFraction(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("83_MineralLayerRateConstant(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("84_DirectRunoffResidenceTime(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("85_UpperSoilResidenceTime(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("86_LowerSoilResidenceTime(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("87_UpperSoilRetentionVolume(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("88_LowerSoilRetentionVolume(").Append(j.ToString()).Append(")").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("89_InstreamInitialFlow").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("90_InstreamInitialDissolvedHg2").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("91_InstreamInitialDissolvedMeHg").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("92_InstreamINitialDissolvedHg0").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("93_InstreamInitialParticulateHg2").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("94_InstreamInitialParticulateMeHg").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("95_InstreamInitialWaterTemperature").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("96_InstreamMinimumWaterTemperature").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("97_InstreamWaterTemperatureLagFactor").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("numberOfReaches").Append("\n");
            for (i = 0; i < numberOfReaches; i++)
            {
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("reachName(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("100_reachLength(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("101_averageWidth(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("102_flow_a(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("103_flow_b(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("104_latitude(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("105_longitude(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("106_ReachHgReduction(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("107_ReachHgMethylation(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("108_ReachHgDemethylation(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("109_ReachHg0Oxidation(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("110_ReachHg0Volatilization(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("111_ReachPhotoReductionMultiplier(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("112_ReachPhotoreductionDOCOffset(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("113_ReachPhotooxidationRadiationMultiplier(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("114_ReachPhotooxidationDOCOffset(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("115_ReachOpenWaterDOCConcentration(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("116_ReachEffluentInputRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("117_ReachFlag0(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("118_ReachFlag1(").Append(i.ToString()).Append(")").Append("\n");
            }
            for (i = 0; i < numberOfReaches; i++)
            {
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("119_CatchmentArea(").Append(i.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++)
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("120_LandscapeProportion(").Append(i.ToString()).Append("_").Append(j.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("125_SubcatchmentBaseFlowIndex(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("126_SubcatchmentThresholdSoilZoneFlow(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("127_SubcatchmentRainfallExcessProportion(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("128_SubcatchmentFlowErosionScalingFactor(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("129_SubcatchmentFlowErosionDirectRunoffThreshold(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("130_SubcatchmentFlowErosionNonlinearTransport(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("131_SubcatchmnetTransportCapacityScalingFactor(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("132_SubcatchmentTransportCapacityDirectRunoffThreshold(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("133_SubcatchmentTransportCapacityNonlinearCoefficient(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("134_SubcatchmentHg0ParticulateDeposition(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("135_SubcatchmentHg2ParticulateDeposition(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("136_SubcatchmentMeHgParticulateDeposition(").Append(i.ToString()).Append(")").Append("\n");
            }
        }
    }
    
    class INCA_C4PParameterArrayList : ParameterArrayList
    {
        public INCA_C4PParameterArrayList(int landUseClasses, int numberOfReaches, int SedimentClasses)
        {
            int i, j, m;
            m = 0;
            header = new StringBuilder();
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ParameterSetName").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SedimentClasses").Append("\n");
            for (j = 0; j < SedimentClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SedimentClassName(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < SedimentClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaxGrainSize(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < SedimentClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MinGrainSize(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < SedimentClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SedimentDensity(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < SedimentClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InmanMultiplier(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < SedimentClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InmanExponent(").Append(j.ToString()).Append(")").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverClasses").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverAbbreviation(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverName(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffInitialFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerInitialFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerInitialFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffInitialPDC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerInitialSOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerInitialDOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerInitialDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerInitialSOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerInitialDOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerInitialDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SedimentStoreInitialCondition(").Append(j.ToString()).Append(")").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("startDate").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("numberOfDays").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SMDMaxForChemistry(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerMinimumFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("VegetationIndex(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaximumInfiltrationRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilErodibilityForSplashDetachment(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilErodibilityForFlowErosion(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SplashDetachmanr_a_Parameter(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PlantGrowthStartDay(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PlantGrowthPeriod(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSnowDepth(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DegreeDayMeltFactor(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SnowWaterEquivalent(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerPDCToSOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerPDCToDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerPDCToDOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerSOCToDOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerSOCToDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerDOCToSOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerDOCToDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerSOCToDOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerSOCToDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerDOCToSOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerDOCToDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerCO2MassTransferVelocity(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DailyPDCInputToOrganicLayer(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LitterfallStartDay(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LitterfallDuration(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LitterfallAmount(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SnowDepthFactor(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("COUP_10DegreeResponse(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("COUP_BaseT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilTThermalConductivity(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilTSpecificHeatCapacity(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MinimumMineralLayerFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerMaxDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerMaxDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerCO2MassTransferVelocity(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerReturnTime(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerEnzymaticLatchRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerCriticalSMD(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerReturnTime(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerEnzymaticLatchRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerCriticalSMD(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerB1(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerB2(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerB1(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerB2(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Delta(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaxSMDForOLRetentionVolume(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaxSMDForMLRetentionVolume(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffTimeConstant(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerTimeConstant(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerTimeConstant(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerTotalToAvailableWaterRatio(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerTotalToAvailableWaterRatio(").Append(j.ToString()).Append(")").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("RiverFlow").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialOpenWaterPDC").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialOpenWaterDOC").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialOpenWaterDIC").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialStreamBedPDC").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialStreamBedDOC").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialStreamBedDIC").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MinimumWaterTemperature").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("numberOfReaches").Append("\n");
            for (i = 0; i < numberOfReaches; i++)
            {
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("CatchmentArea(").Append(i.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++)
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Land(").Append(i.ToString()).Append("_").Append(j.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DeltaMG(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ThresholdSoilZoneFlow(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("RainfallExcessProportion(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FlowErosionScalingFactor(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FlowErosionDirectRunoffThreshold(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FlowErosionNonLinearCoefficient(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("TransportCapacityScalingFactor(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("TransportCapacityDirectRunoffThreshold(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("TransportCapacityNonLinearCoefficient(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterSustainableFlow(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterTimeConstant(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterMaximumEffectiveDepth(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ProportionOfFilledPoreSpace(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterInitialFlow(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MeanSlope(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("CO2SaturationConcentration(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AtmosphericPartialPressureOfCO2(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("HenrysLawConstantforCO2(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DryDeposition(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WetDeposition(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterInitialSOC(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterInitialDOC(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterInitialDIC(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DeltaOM(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DeltaOG(").Append(i.ToString()).Append(")").Append("\n");
            }
            for (i = 0; i < numberOfReaches; i++)
            {
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("reachName(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("reachID(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("reachLength(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("averageWidth(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("slope(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("flow_a(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("flow_b(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("a7(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("a8(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("a9(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("a10(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("latitude(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("longitude(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachArea(").Append(i.ToString()).Append(")").Append("\n");

                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OpenWaterPDCSettlingVelocity(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DOCToDICSelfShadingFactor(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DOCToDICRadiationMultiplier(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OpenWaterDOCToDICMicrobial(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OpenWaterPDCToDOC(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OpenWaterDOCToPDC(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OpenWaterDICMassTransferRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DICToPDCPhotosynthesis(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Porosity(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BedDepth(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WaterColumnToPoreWaterCExchange(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StreamBedDOCToPDC(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StreamBedPDCToDOC(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StreamBedPDCMineralization(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StreamBedDOCMineralization(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AbstractionRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentInputRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentConcentration(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LitterFallOntoReach(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentDICConcentration(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AbstractionOnOffSwitch(").Append(i.ToString()).Append(")").Append("\n");
            }
        }
    }

    class INCA_ToxParameterArrayList : ParameterArrayList
    {
        //updated to 1.0.x
        public INCA_ToxParameterArrayList(int landUseClasses, int numberOfReaches, int numberOfGrainSizes, int numberOfContaminants)
        {
            int h, i, j, k, m;
            m = 0;
            header = new StringBuilder();
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ParameterSetName").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NumberOfGrainSsizeClasses").Append("\n");
            for (h = 0; h < numberOfGrainSizes; h++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GrainSizeClassName(").Append(h.ToString()).Append(")").Append("\n");
            for (h = 0; h < numberOfGrainSizes; h++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaximumDiameter_um(").Append(h.ToString()).Append(")").Append("\n");
            for (h = 0; h < numberOfGrainSizes; h++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MinimumDiameter_um(").Append(h.ToString()).Append(")").Append("\n");
            for (h = 0; h < numberOfGrainSizes; h++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Density(").Append(h.ToString()).Append(")").Append("\n");
            for (h = 0; h < numberOfGrainSizes; h++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InmanParameter1(").Append(h.ToString()).Append(")").Append("\n");
            for (h = 0; h < numberOfGrainSizes; h++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InmanParameter2(").Append(h.ToString()).Append(")").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NumberOfContaminants").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverAbbreviation(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverName(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffInitialFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerInitialFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerInitialFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffInitialPDC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerInitialSOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerInitialDOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerInitialDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerInitialSOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerInitialDOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerInitialDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SedimentStoreInitialCondition(").Append(j.ToString()).Append(")").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("startDate").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("numberOfDays").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SMDMaxForChemistry(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerMinimumFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("VegetationIndex(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaximumInfiltrationRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilErodibilityForSplashDetachment(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilErodibilityForFlowErosion(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SplashDetachmant_a_Parameter(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PlantGrowthStartDay(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PlantGrowthPeriod(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSnowDepth(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DegreeDayMeltFactor(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SnowWaterEquivalent(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerPDCToSOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerPDCToDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerPDCToDOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerSOCToDOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerSOCToDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerDOCToSOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerDOCToDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerSOCToDOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerSOCToDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerDOCToSOC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerDOCToDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerCO2MassTransferVelocity(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DailyPDCInputToOrganicLayer(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LitterfallStartDay(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LitterfallDuration(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LitterfallAmount(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaximumTDifference(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SnowDepthFactor(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("COUP_10DegreeResponse(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("COUP_BaseT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilTThermalConductivity(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilTSpecificHeatCapacity(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MinimumMineralLayerFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerMaxDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerMaxDIC(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerCO2MassTransferVelocity(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerReturnTime(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerEnzymaticLatchRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerCriticalSMD(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerReturnTime(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerEnzymaticLatchRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerCriticalSMD(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerB1(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerB2(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerB1(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerB2(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Delta(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaxSMDForOLRetentionVolume(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaxSMDForMLRetentionVolume(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSoilT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EasilyAccessibleFraction(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrgaqnicLayerPorosity(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerPorosity(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("TBLPathLength(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("CBLPathLength(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WindSpeedTDCoefficient(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffectiveBioturbationCoefficient(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerDepth(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerDepth(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GaseousExchangeFraction(").Append(j.ToString()).Append(")").Append("\n");
            for (h = 0; h < numberOfGrainSizes; h++)
            {
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GrainSizePct(").Append(h.ToString()).Append(MCParameters.dot).Append(j.ToString()).Append(")").Append("\n");
            }

            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffTimeConstant(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerTimeConstant(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerTimeConstant(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicLayerTotalToAvailableWaterRatio(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralLayerTotalToAvailableWaterRatio(").Append(j.ToString()).Append(")").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NumberOfContaminants").Append("\n");
            for (i = 0; i < numberOfContaminants; i++)
            {
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ContaminantName(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SWHenrysLawConstant(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SWEnthalpyAW(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SWOctanolWater(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SWEnthalpyOW(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SWEasyToPotentialTimeConstant(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MolarVolume(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MolarMass(").Append(i.ToString()).Append(")").Append("\n");
                for (h = 0; h < numberOfGrainSizes; h++)
                {
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GrainSizekSOCFactor(").Append(i.ToString()).Append(MCParameters.dot).Append(h.ToString()).Append(")").Append("\n");
                }
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("UpperLayerInitialBulkConcentration(").Append(i.ToString()).Append(MCParameters.dot).Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LowerLayerInitialBulkConcentration(").Append(i.ToString()).Append(MCParameters.dot).Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AtmosphericDryParticleDeposition(").Append(i.ToString()).Append(MCParameters.dot).Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AtmosphericWetDeposition(").Append(i.ToString()).Append(MCParameters.dot).Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MassAppliedInFertilizer(").Append(i.ToString()).Append(MCParameters.dot).Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MassInLitterfall(").Append(i.ToString()).Append(MCParameters.dot).Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("TimeToEquilibrate(").Append(i.ToString()).Append(MCParameters.dot).Append(j.ToString()).Append(")").Append("\n");
            }
            for (int r = 0; r < numberOfContaminants; r++)
            {
                for (int c = 0; c < numberOfContaminants; c++)
                {
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FormationMatrix(").Append(r.ToString()).Append(MCParameters.dot).Append(c.ToString()).Append(")").Append("\n");
                }
            }
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialFlow").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OpenWaterPDC").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OpenWaterDOC").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OpenWaterDIC").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StreambedPDC").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StreambedDOC").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StreambedDIC").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialWaterTemperature").Append("\n");
            for (h = 0; h < numberOfGrainSizes; h++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSSConcentration(").Append(h.ToString()).Append(")").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MinimumWaterTemperature").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WaterTemperatureLagFactor").Append("\n");
            for (i = 0; i < numberOfContaminants; i++)
            {
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WaterColumnInitialBulkConcentration(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("UpperSedimentInitialBulkConcentration(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LowerSedimentInitialBulkConcentration(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InWaterDegradationHalfLife(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ShelteringFactor(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SolidSedimentDegradationHalfLife(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FreelyDissolvedSedimentDegradationHalfLife(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OpenWaterTimeToEquilibrate(").Append(i.ToString()).Append(")").Append("\n");
             }
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NumberOfReaches").Append("\n");
            for (k = 0; k < numberOfReaches; k++)
            {
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachID(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachName(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SubCatchmentArea(").Append(k.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverPct(").Append(k.ToString()).Append(MCParameters.dot).Append(j.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DeltaMG(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ThresholdSoilZoneFlow(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("RainfallExcessProportion(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FlowErosionScalingFactor(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FlowErosionDirectRunoffThreshold(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FlowErosionNonLinearCoefficient(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("TransportCapacityScalingFactor(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("TransportCapacityDirectRunoffThreshold(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("TransportCapacityNonLinearCoefficient(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterSustainableFlow(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterTimeConstant(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaximumGroundwaterRetentionVolume(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterProportion(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterInitialFlow(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MeanSlope(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("CO2SaturationConcentration(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AtmosphericCO2PartialPressure(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("HenrysLawConstantForCO2(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SO4DryDeposition(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SO4WetDeposition(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterInitialSOC(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterInitialDOC(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterInitialDIC(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DeltaOM(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DeltaOG(").Append(k.ToString()).Append(")").Append("\n");
                for (i = 0; i < numberOfContaminants; i++)
                {
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OLEADegradationHalfLife(").Append(k.ToString()).Append(MCParameters.dot).Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OLPADegradationHalfLife(").Append(k.ToString()).Append(MCParameters.dot).Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MLEADegradationHalfLife(").Append(k.ToString()).Append(MCParameters.dot).Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MLPADegradationHalfLife(").Append(k.ToString()).Append(MCParameters.dot).Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AtmosphericConcentration(").Append(k.ToString()).Append(MCParameters.dot).Append(i.ToString()).Append(")").Append("\n");
                }
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachLength(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachWidth(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachSlope(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Flow_a(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Flow_b(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("a7(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("a8(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("a9(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("a10(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Latitude(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Longitude(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachArea(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OpenWaterPDCSettlingVelocity(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OpenWaterDocToDICSelfShadingFactor(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OpenWaterDOCToDICRadiationMultiplier(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OpenWaterMicrobialDOCToDIC(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OpenWaterPDCToDOC(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OpenWaterDOCToPDC(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OpenWaterDICMassTransfer(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OpenWaterDICToPDC(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BedPorosity(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BedDepth(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WaterColumnToPoreWaterCExchange(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StreamBedDOCToPDC(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StreamBedPDCToDOC(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StreamBedPDCMineralization(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StreamBedDOCMineralization(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AbstractionFlowRate(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentFlowRate(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentDOCConcentration(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachLitterfallRate(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentDICConcentraiton(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentOnOff(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("isWaterfall(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("hE(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("dS(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WaterfallHeight(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("dp(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SedimentEffectiveBioturbationCoefficient(").Append(k.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BioturbationSizeThreshold(").Append(k.ToString()).Append(")").Append("\n");
                for (h = 0; h < numberOfGrainSizes; h++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GrainSizeEffluentConcentration(").Append(k.ToString()).Append(MCParameters.dot).Append(h.ToString()).Append(")").Append("\n");
                for (h = 0; h < numberOfGrainSizes; h++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialBedSedimentGrainSizeClass(").Append(k.ToString()).Append(MCParameters.dot).Append(h.ToString()).Append(")").Append("\n");
                for (h = 0; h < numberOfGrainSizes; h++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GrainSizeOCFraction(").Append(k.ToString()).Append(MCParameters.dot).Append(h.ToString()).Append(")").Append("\n");
                for (i = 0; i < numberOfContaminants; i++)
                {
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FreelyDissolvedEffluentConcentration(").Append(k.ToString()).Append(MCParameters.dot).Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DOCAssociatedEffluentConcentration(").Append(k.ToString()).Append(MCParameters.dot).Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SOCAssociatedEffluentConcentration(").Append(k.ToString()).Append(MCParameters.dot).Append(i.ToString()).Append(")").Append("\n");
                }
            }
        }
    }

    class INCA_PParameterArrayList : ParameterArrayList // updated to v1.4
        {
            public INCA_PParameterArrayList(int landUseClasses, int numberOfReaches)
            {
                int i, j, m;
                m = 0;
                header = new StringBuilder();
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ParameterSetName").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverAbbreviation(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverName(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoff(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilWaterFlow(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SedimentStore(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilWaterTDP(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilInactiveP(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilLabileP(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffTDP(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilSorptionScalingFactor(").Append(j.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("startDate").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("numberOfDays").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SMDMax(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SWMaxSustainableFlow(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("VegetationIndex(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ClayPct(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SiltPct(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FineSandPct(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MediumSandPct(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("CoarseSandPct(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaxInfiltrationRate(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SplashDetachmentSoilErodibilityParameter(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FlowErosionSoilErodibilityParameter(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SplashDetachment_a(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PlantGrowthStartDay(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PlantGrowthPeriod(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSnowDepth(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DegreeDaySnowmelt(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SWE(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaximumTDiff(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FertiliserAdditionStartDay(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FertiliserAdditionPeriod(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PlantResidue(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SolidManureAndSlurry(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SolidFertiliser(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LiquidManureAndSlurry(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LiquidFertiliser(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SnowDepthSoilTFactor(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ResponseToTChange(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BaseTResponse(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ThermalConductivityOfSoil(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SpecificHeatCapacityDueToFreezeThaw(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GrowthCurveOffset(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GrowthCurveAmplitude(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PlantPUptake(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ImmobilisationRate(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaximumUptake(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ImmobilisationTThreshold(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FreundlichIsothermConstant(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WeatheringFactor(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SorptionCoefficientForSoilMatrix(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSoilT(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilDepth(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilBulkDensity(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LabilePSaturationThreshold(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DailyMaximumPUptake(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EnrichmentRatio(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffTimeConstant(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilReactiveZoneTimeConstant(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilWaterRetentionVolumeConstant(").Append(j.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Flow").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WaterColumnTDP").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StreamBedTDP").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WaterColumnPP").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StreamBedPP").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MacrophyteMass").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EpiphyteMass").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialWaterT").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSuspendedCoarseSand").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSuspendedMediumSand").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSuspendedFineSand").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSuspendedSilt").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSuspendedClay").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MinimumWaterT").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SuspendedSedimentPSaturationThreshold").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BedSedimentPSaturationThreshold").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WaterTLagFactor").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NumberOfReaches").Append("\n");
                for (i = 0; i < numberOfReaches; i++)
                {
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("reachID(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("reachName(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SubcatchmentArea(").Append(i.ToString()).Append(")").Append("\n");
                    for (j = 0; j < landUseClasses; j++)
                        header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Land(").Append(i.ToString()).Append("_").Append(j.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BaseFlowIndex(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ThresholdSoilZoneFlow(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("RainfallExcessProportion(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FlowErosionScalingFactor(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FlowErosionDirectRunoffThreshold(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FlowErosionNonLinearCoefficient(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("TransportCapacityScalingFactor(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("TransportCapacityDirectRunoffThreshold(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("TransportCapacityNonLinearCoefficieint(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterSustainableFlow(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterTimeConstant(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaxGroundwaterRetentionVolume(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterProportion(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterInitialFlow(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SorptionScalingFactor(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AquiferMatrixSorptionCoefficient(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AquiferFreundlichConstant(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AquiferBulkDensity(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialGroundwaterTDP(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialGroundwaterInactiveP(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("TotalSolidPSaturationThreshold(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicPDryDeposition(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicPWetDeposition(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InorganicPDryDeposition(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InorganicPWetDeposition(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Length(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Width(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Slope(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Flow_a(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Flow_b(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("CoarseSandEffluentConcentration(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MediumSandEffluentConcentration(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FineSandEffluentConcentration(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SiltEffluentConcentration(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ClayEffluentConcentration(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentFlowRate(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AbstractionFlowRate(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("a7(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("a8(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("a9(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("a10(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Latitude(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Longitude(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MacrophyteTDependency(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EpiphyteTDependency(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Porosity(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MacrophyteGrowthRate(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MacrophyteDeathRate(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EpiphyteGrowthRate(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EpiphyteDeathRate(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ProportionOfPInEpiphytes(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("HalfSaturationOfPForEpihpyteGrowth(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PEexchange(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("HalfSaturationOfPForMacrophyteGrowth(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MacrophyteSelfShadingConstant(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ProportionOfPInMacrophytes(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WaterColumnAdsorptionCoefficient(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StreamBedAdsorptionCoefficient(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WaterColumnSorptionScalingFactor(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StreamBedSorptionScalingFactor(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WaterColumnFreundlichIsothermConstant(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StreamBedFreundlichIsothermConstant(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("TDPEffluentConcentration(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PPEffluentConcentration(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BedDepth(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialCoarseSand(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialMediumSand(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialFineSand(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSilt(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialClay(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentSwitch(").Append(i.ToString()).Append(")").Append("\n");
                }
            }
        }

    class INCA_ONTHEParameterArrayList : ParameterArrayList // updated to v1.4
    {
        public INCA_ONTHEParameterArrayList(int landUseClasses, int numberOfReaches)
        {
            int i, j, m;
            m = 0;
            header = new StringBuilder();
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ParameterSetName").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverAbbreviation(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverName(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffInitialFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilWaterFlowInitialFLow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffInitialNitrate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilWaterFlowInitialNitrate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffInitialAmmonium(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilWaterFlowInitialAmmonium(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilWaterFlowInitialDON(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilWaterInitialSON(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffInitialDON(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilWaterInitialWaterDepth(").Append(j.ToString()).Append(")").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("startDate").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("numberOfDays").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilwaterDenitrificationRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilwaterNFixationRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PlantGrowthNitrateUptakeRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PlantGrowthMaximumNUptake(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FertiliserNitrateAdditionRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilwaterNitrificationRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilwaterMineralisationRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilwaterImmobilisationRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FertiliserAmmoniumAdditionRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PlantGrowthAmmoniumUptakeRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GrowthSeasonStartDay(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GrowthPeriod(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FertiliserAdditionStartDay(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FertiliserAdditionPeriod(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaximumSMD(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DifferenceBetweenSoilTMaxima(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StopDenitrificationT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StopNitrificaitionT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StopMineralisationT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StopImmobilisationT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MinimumSoilwaterFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MinimumDirectRunoffFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DenitrificationTResponse(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DenitrificationBaseT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSnowDepth(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DegreeDaySnowmelt(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SWE(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SnowDepthFactor(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilThermalConductivity(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilSpecificHeatCapacity(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NitrateHalfSaturation(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AmmoniumHalfSaturation(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FixationTResponse(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FixationBaseT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NitrificationTResponse(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NitrificationBaseT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralisationTResponse(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralisationBaseT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ImmobilisationTResponse(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ImmobilisationBaseT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NitrateUptakeTResponse(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NitrateUptakeBaseT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AmmoniumUptakeTResponse(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AmmoniumUptakeBaseT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilwaterSONFreundlichkSD(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilwaterSONFreundlichn(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilwaterSONFreundlichkF(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("OrganicNAdditionRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilBulkPorosity(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverRainMultiplier(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverSnowMultiplier(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandcoverSnowmeltOffset(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Interception(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("TemperatureOffsetForPET(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ScalingFactorForPET(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DroughtRunoffProportion(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EvapotranspirationExponent(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SMDOffset(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ThresholdSEFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("RainfallExcessProportion(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaximumInfiltrationRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InfiltrationTThreshold(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilwaterInertDepth(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffCharacteristicTimeConstant(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilwaterCharacteristicTimeConstant(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilwaterRatioOfTotalToAvailableWater(").Append(j.ToString()).Append(")").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Flow").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialInstreamNitrate").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialInstreamAmmonium").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialInstreamOrganicN").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialInstreamT").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NumberOfReaches").Append("\n");
            for (i = 0; i < numberOfReaches; i++)
            {
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("reachID(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("reachName(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SubcatchmentArea(").Append(i.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++)
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Land(").Append(i.ToString()).Append("_").Append(j.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BaseFlowIndex(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterDenitrification(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterMaximumEffectiveDepth(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterProportionOfFilledPoreSpace(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterCharacteristicTimeConstant(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterInitialFlow(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterInitialNitrate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterInitialAmmonium(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MinimumGroundwaterFlow(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterMineralizationRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterInitialDON(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterNitrificationRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SubcatchmentRainMultiplier(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SubcatchmentSnowMultiplier(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SubcatchmentSnowThresholdT(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SubcatchmentSnowmeltOffset(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SubcatchmentThresholdSoilZoneFlow(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SubcatchmentRainfallExcessProportion(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SubcatchmentNitrateDryDeposition(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SubcatchmentNitrateWetDeposition(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SubcatchmentAmmoniumDryDeposition(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SubcatchmentAmmoniumWetDeposition(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachLength(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Manning_a(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Manning_b(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InstreamDenitrificationRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InstreamNitrificationRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentNitrate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentAmmonium(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentDischarge(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AbstractionRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentAbstractionFlag(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Latitude(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Longitude(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Mannings_c(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Mannings_f(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ChannelWidthAtSedimentSurface(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Mannings_n(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ChannelSlope(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InstreamMineralisationRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InstreamImmobilisationRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentDON(").Append(i.ToString()).Append(")").Append("\n");
            }
        }
    }

    class INCA_NParameterArrayList : ParameterArrayList // updated to v1.4
    {
        public INCA_NParameterArrayList(int landUseClasses, int numberOfReaches)
        {
            int i, j, m;
            m = 0;
            header = new StringBuilder();
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ParameterSetName").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverAbbreviation(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverName(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffInitialFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilWaterFlowInitialFLow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffInitialNitrate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilWaterFlowInitialNitrate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffInitialAmmonium(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilWaterFlowInitialAmmonium(").Append(j.ToString()).Append(")").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("startDate").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("numberOfDays").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilwaterDenitrificationRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilwaterNFixationRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PlantGrowthNitrateUptakeRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PlantGrowthMaximumNUptake(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FertiliserNitrateAdditionRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilwaterNitrificationRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilwaterMineralisationRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilwaterImmobilisationRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FertiliserAmmoniumAdditionRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PlantGrowthAmmoniumUptakeRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GrowthSeasonStartDay(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GrowthPeriod(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FertiliserAdditionStartDay(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FertiliserAdditionPeriod(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaximumSMD(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DifferenceBetweenSoilTMaxima(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StopDenitrificationT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StopNitrificaitionT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StopMineralisationT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StopImmobilisationT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MinimumSoilwaterFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MinimumDirectRunoffFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DenitrificationTResponse(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DenitrificationBaseT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSnowDepth(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DegreeDaySnowmelt(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SWE(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SnowDepthFactor(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilThermalConductivity(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilSpecificHeatCapacity(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GrowthCurveOffset(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GrowthCurveAmplitude(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FixationTResponse(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FixationBaseT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NitrificationTResponse(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NitrificationBaseT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralisationTResponse(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralisationBaseT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ImmobilisationTResponse(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ImmobilisationBaseT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NitrateUptakeTResponse(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NitrateUptakeBaseT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AmmoniumUptakeTResponse(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AmmoniumUptakeBaseT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffCharacteristicTimeConstant(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilwaterCharacteristicTimeConstant(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilwaterRatioOfTotalToAvailableWater(").Append(j.ToString()).Append(")").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialStreamFlow").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialInstreamNitrate").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialInstreamAmmonium").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialStreamTemperature").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NumberOfReaches").Append("\n");
            for (i = 0; i < numberOfReaches; i++)
            {
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("reachID(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("reachName(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SubcatchmentArea(").Append(i.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++)
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Land(").Append(i.ToString()).Append("_").Append(j.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BaseFlowIndex(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ThresholdSoilZoneFlow(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("RainfallExcessProportion(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaximumInfiltrationRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterDenitrification(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterMaximumEffectiveDepth(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterProportionOfFilledPoreSpace(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterCharacteristicTimeConstant(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterInitialFlow(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterInitialNitrate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterInitialAmmonium(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MinimumGroundwaterFlow(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SubcatchmentNitrateDryDeposition(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SubcatchmentNitrateWetDeposition(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SubcatchmentAmmoniumDryDeposition(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SubcatchmentAmmoniumWetDeposition(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachLength(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Flow_a(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Flow_b(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InstreamDenitrificationRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InstreamNitrificationRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentNitrate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentAmmonium(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentDischarge(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AbstractionRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentAbstractionFlag(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Latitude(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Longitude(").Append(i.ToString()).Append(")").Append("\n");
            }
        }
    }
    class INCA_PEcoParameterArrayList : ParameterArrayList // updated to v2.0, missing a reference to Algal maximum SRP for growth
    {
        public INCA_PEcoParameterArrayList(int landUseClasses, int numberOfReaches)
        {
            int i, j, m;
            m = 0;
            header = new StringBuilder();
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ParameterSetName").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverAbbreviation(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverName(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffInitialFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilWaterInitialFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSedimentStore(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilWaterInitialTDP(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilInitialInactiveP(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilInitialLabileP(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffInitialTDP(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilSorptionScalingFactor(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilwaterInitialBODConcentration(").Append(j.ToString()).Append(")").Append("\n");

            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("startDate").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("numberOfDays").Append("\n");
            
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SMDMax(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SWMinSustainableFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("VegetationIndex(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("CoarseSandPct(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MediumSandPctPct(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FineSandPct(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SiltPct(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ClayPct(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaxInfiltrationRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SplashDetachmentSoilErodibilityParameter(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FlowErosionSoilErodibilityParameter(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SplashDetachment_a(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PlantGrowthStartDay(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PlantGrowthPeriod(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSnowDepth(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DegreeDaySnowmelt(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SWE(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaximumTDiff(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FertiliserAdditionStartDay(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FertiliserAdditionPeriod(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PlantResidueInputs(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SolidManureAndSlurryInputs(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SolidMineralFertiliserInputs(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LiquidManureAndSlurryInputs(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LiquidMineralFertiliserInputs(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SnowDepthSoilTFactor(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ResponseToTChange(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BaseTResponse(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ThermalConductivityOfSoil(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SpecificHeatCapacityDueToFreezeThaw(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GrowthCurveOffset(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GrowthCurveAmplitude(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PlantPUptake(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ImmobilisationRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("TotalMaximumPUptake(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ImmobilisationTThreshold(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FreundlichIsothermConstant(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WeatheringFactor(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SorptionCoefficientForSoilMatrix(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSoilT(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilDepth(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilBulkDensity(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LabilePSaturationThreshold(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DailyMaximumPUptake(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EnrichmentRatio(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BODDecayRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BODForPlantResidue(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BODForSolidManureAndSlurry(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BODForAnimalFeces(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffDOConcentration(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilwaterDOConcentration(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffTimeConstant(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilReactiveZoneTimeConstant(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilWaterRatioOfTotalToAvailableWater(").Append(j.ToString()).Append(")").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InstreamInitialFlow").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InstreamInitialWaterColumnTDP").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InstreamInitialStreamBedTDP").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InstreamInitialWaterColumnPP").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InstreamInitialStreamBedPP").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InstreamInitialMacrophyteMass").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InstreamInitialEpiphyteMass").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InstreamInitialWaterT").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSuspendedCoarseSand").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSuspendedMediumSand").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSuspendedFineSand").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSuspendedSilt").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSuspendedClay").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialLivePhytoplankton").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialDO").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialBOD").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MinimumWaterT").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SuspendedSedimentPSaturationThreshold").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BedSedimentPSaturationThreshold").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WaterTLagFactor").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NumberOfReaches").Append("\n");
            for (i = 0; i < numberOfReaches; i++)
            {
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("reachID(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("reachName(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SubcatchmentArea(").Append(i.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++)
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Land(").Append(i.ToString()).Append("_").Append(j.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BaseFlowIndex(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ThresholdSoilZoneFlow(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("RainfallExcessProportion(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FlowErosionScalingFactor(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FlowErosionDirectRunoffThreshold(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FlowErosionNonLinearCoefficient(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("TransportCapacityScalingFactor(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("TransportCapacityDirectRunoffThreshold(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("TransportCapacityNonLinearCoefficieint(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterSustainableFlow(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterTimeConstant(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaxGroundwaterMaxEffectuveDepth(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterProportionOfFilledPoreSpace(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterInitialFlow(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterSorptionScalingFactor(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AquiferMatrixSorptionCoefficient(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AquiferFreundlichConstant(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AquiferMass(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialGroundwaterTDP(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialGroundwaterTotalSolidPConcentration(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("TotalSolidPSaturationThreshold(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterBODDecay(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterInitialBODConcentration(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterInitialDOConcentration(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("TotalDryPDeposition(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("TotalWetPDeposition(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("UnusedParameterA(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("UnusedParameterB(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Length(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WidthAtSedimentSurface(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Slope(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Manning_a(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Manning_b(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentCoarseSandConcentration(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentMediumSandConcentration(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentFineSandConcentration(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentSiltConcentration(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentClayConcentration(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentFlowRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AbstractionFlowRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StreambedShearVelocity_a7(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StreambedScalingFactor_a8(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StreambedBackgroundPReleaseFactor_a9(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StreambedNonlinearCoefficient_a10(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Latitude(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Longitude(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MacrophyteTDependency(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EpiphyteTDependency(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StreambedPorosity(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MacrophyteGrowthRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MacrophyteDeathRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EpiphyteGrowthRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EpiphyteDeathRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ProportionOfPInEpiphytes(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("HalfSaturationOfPForEpihpyteGrowth(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WaterColumnPEexchange(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("HalfSaturationOfPForMacrophyteGrowth(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MacrophyteSelfShadingConstant(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ProportionOfPInMacrophytes(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WaterColumnSorptionCoefficient(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StreamBedSorptionCoefficient(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WaterColumnSorptionScalingFactor(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StreamBedSorptionScalingFactor(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WaterColumnFreundlichIsothermConstant(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StreamBedFreundlichIsothermConstant(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentTDPConcentration(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentPPConcentration(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BedDepth(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BedSedimentInitialCoarseSand(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BedSedimentInitialMediumSand(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BedSedimentInitialFineSand(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BedSedimentInitialSilt(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BedSedimentInitialClay(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AlgalDeathRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AlgalSelfShadingConstant(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AlgalRespirationOffset(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AlgalRespirationSlope(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AlgalGrowthRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SedimentOxidation(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NetBODSettlingVelocity(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LowChlAlgalPhotosynthesisRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("HighChlAlgalPhotosynthesisRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BODDecay(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentLivePhytoplanktonConcentration(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentDOConcentration(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentBODConcentration(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AtmosphericReaeration(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ContributionOfDeadAlgaeToBOD(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AlgalHalfSaturationForPGrowth(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AlgalTemperatureThresholdForGrowth(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AlgalSolarRadiationOffsetForGrowth(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("UnknownPlaceholder(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Manning_c(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Manning_f(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Manning_n(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentSwitch(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AbstractionSwitch(").Append(i.ToString()).Append(")").Append("\n");
            }
        }
    }

    class INCA_PathParameterArrayList : ParameterArrayList
    {
        public INCA_PathParameterArrayList(int landUseClasses, int numberOfReaches)
        {
            int i, j, m;
            m = 0;
            header = new StringBuilder();
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ParameterSetName").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverAbbreviation(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverName(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoff(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilWaterFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffPathogen(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilWaterPathogen(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilTemperature(").Append(j.ToString()).Append(")").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("startDate").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("numberOfDays").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PlantGrowthStartDay(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PlantGrowthPeriod(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SMDMax(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaximumTDiff(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilWaterSustainableFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffSustainableFlow(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilT10Response(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BaseTResponse(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSnowPackDepth(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DegreeDayFactorForSnowmelt(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WaterEquivalentFactor(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SnowDepthSoilTFactor(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilThermalConductivity(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SpecificHeatFreezeThaw(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GrowthCurveOffset(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GrowthCurveAmplitude(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilWaterPathogenDieOffRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilWaterPathogenGrowthRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LightDecayProportionalityConstant(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AnimalAdditionRate(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AnimalAdditionStartDay1(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AnimalAdditionPeriod1(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AnimalAdditionRate1(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AnimalAdditionStartDay2(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AnimalAdditionPeriod2(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AnimalAdditionRate2(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AnimalAdditionStartDay3(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AnimalAdditionPeriod3(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AnimalAdditionRate3(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffTimeConstant(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilReactiveZoneTimeConstant(").Append(j.ToString()).Append(")").Append("\n");
            for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("TotalToAvailableWater(").Append(j.ToString()).Append(")").Append("\n");

            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialFlow").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialWaterColumnPathogen").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSedimentPathogen").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialWaterT").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MinimumWaterT").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WaterTLagFactor").Append("\n");
            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NumberOfReaches").Append("\n");
            for (i = 0; i < numberOfReaches; i++)
            {
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("reachID(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("reachName(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SubcatchmentArea(").Append(i.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++)
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Land(").Append(i.ToString()).Append("_").Append(j.ToString()).Append(")").Append("\n");

                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BaseFlowIndex(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ThresholdSoilZoneFlow(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("RainfallExcessProportion(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaximumInfiltrationRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterMaxEffectiveDepth(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterFilledPoreSpace(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterTimeConstant(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaxGroundwaterInitialFlow(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterSustainableFlow(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterPathogenDieOffRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterPathogenGrowthRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterPathogenInitialConcentration(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DryDeposition(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WetDeposition(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Length(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Flow_a(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Flow_b(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentFlowRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AbstractionFlowRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Latitude(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Longitude(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Width(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Slope(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ShearVelocityScalingFactor(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LightDecayProportionalityConstant(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DepositionRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ShearVelocityThresholdForResuspension(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ResuspensionRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WaterColumnDecayRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("WaterColumnGrowthRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SedimentDecayRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SedimentGrowthRate(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentConcentration(").Append(i.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentSwitch(").Append(i.ToString()).Append(")").Append("\n");
            }
        }
    }

    class Branching_INCA_N_With_PERSiSTParameterArrayList : ParameterArrayList
        {
            public Branching_INCA_N_With_PERSiSTParameterArrayList(int landUseClasses, int numberOfReaches, int numberOfBoxes)
            {
                int i, j, k, l, m, n;
                m = 0;
                header = new StringBuilder();
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ParameterSetName").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverAbbreviation(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverName(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffInitialFlow(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilWaterInitialFlow(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffInitialNitrate(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilWaterInitialNitrate(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffInitialAmmonium(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilWaterInitialAmmonium(").Append(j.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("startDate").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("numberOfDays").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilWaterDenitrification(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NitrogenFixation(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PlantNitrateUptakeRate(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaximumNitrogenUptakeRate(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NitrateAdditionRate(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AmmoniumNitrificationRate(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AmmoniumMineralisationRate(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AmmoniumImmobilisationRate(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AmmoniumAdditionRate(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PlantAmmoniumUptakeRate(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PlantGrowthStartDay(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PlantGrowthPeriod(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FertiliserAdditionStartDay(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FertiliserAdditionPeriod(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SMDMax(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaximumTDifference(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DenitrificationTThreshold(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NitrificationTThreshold(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralisationTThreshold(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ImmobilisationTThreshold(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilWaterSustainableFlow(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectRunoffSustainableFlow(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DenitrificationTResponse(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DenitrificationBaseT(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSnowDepth(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DegreeDaySnowmelt(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SnowWaterEquivalentFactor(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SnowDepthSoilTFactor(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilThermalConductivity(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("FreezeThawSpecificHeatCapacity(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GrowthCurveOffset(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GrowthCurveAmplitude(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NFixationTResponse(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NFixationBaseT(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NitrificationTResponse(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NitrificationBaseT(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralisationTResponse(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MineralisationBaseT(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ImmobilisationTResponse(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ImmobilisationBaseT(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NitrateUptakeTResponse(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NitrateUptakeBaseT(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AmmoniumUptakeTResponse(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AmmoniunUptakeBaseT(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ZeroRateDepth(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaxRateDepth(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DirectTimeConstant(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilTimeConstant(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("TotalToAvailableRatio(").Append(j.ToString()).Append(")").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialFlow").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialNitrate").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialAmmonium").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MinimumWaterT").Append("\n");
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NumberOfReaches").Append("\n");


                for (i = 0; i < numberOfReaches; i++)
                {
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachNumber(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachName(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachArea(").Append(i.ToString()).Append(")").Append("\n");
                    for (k = 0; k < landUseClasses; k++)
                    {
                        header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandUse(").Append(i.ToString()).Append(")(").Append(k.ToString()).Append("\n");
                    }
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("BaseFlowIndex(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ThresholdSoilZoneFlow(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("RainfallExcessProportion(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaximumInfiltrationRate(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterDenitrificationRate(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaxGroundwaterEffectiveDepth(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ProportionOfFilledPoreSpace(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterResidenceTime(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialGroundwaterFlow(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialGroundwaterNitrate(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialGroundwaterAmmonium(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GroundwaterSustainableFlow(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NitrateDryDeposition(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("NitrateWetDeposition(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AmmoniumDryDeposition(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AmmoniumWetDeposition(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachLength(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Flow_a(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Flow_b(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InstreamNitrification(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InstreamDenitrification(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentNitrateConcentration(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentAmmoniumConcentration(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("EffluentFlow(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AbstractionRate(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("HasAbstractionOrEffluent(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Latitude(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Longitude(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("PrecipitationAsSnow(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachSnowMultiplier(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ReachRainMultiplier(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("StreamWidth(").Append(i.ToString()).Append(")").Append("\n");
                }
                header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("UnknownNumber").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SnowMultiplier(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SnowMeltT(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DegreeDayMeltFactor(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("RainMultiplier(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialSnowDepth(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("GrowingDegreeThreshold(").Append(j.ToString()).Append(")").Append("\n");
                for (j = 0; j < landUseClasses; j++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DegreeDayEvapotranspiration(").Append(j.ToString()).Append(")").Append("\n");
                for (i = 0; i < numberOfReaches; i++)
                {
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SoilBoxName(").Append(i.ToString()).Append(")").Append("\n");
                    for (k = 0; k < landUseClasses; k++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InitialWaterDepth(").Append(i.ToString()).Append(")(").Append(k.ToString()).Append("\n");
                    for (k = 0; k < landUseClasses; k++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("RelativeAreaIndex(").Append(i.ToString()).Append(")(").Append(k.ToString()).Append("\n");
                    for (k = 0; k < landUseClasses; k++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InfiltrationCapacity(").Append(i.ToString()).Append(")(").Append(k.ToString()).Append("\n");
                    for (k = 0; k < landUseClasses; k++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("RetainedWaterDepth(").Append(i.ToString()).Append(")(").Append(k.ToString()).Append("\n");
                    for (k = 0; k < landUseClasses; k++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("DroughtRunofFraction(").Append(i.ToString()).Append(")(").Append(k.ToString()).Append("\n");
                    for (k = 0; k < landUseClasses; k++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ResidenceTime(").Append(i.ToString()).Append(")(").Append(k.ToString()).Append("\n");
                    for (k = 0; k < landUseClasses; k++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("ETAdjustment(").Append(i.ToString()).Append(")(").Append(k.ToString()).Append("\n");
                    for (k = 0; k < landUseClasses; k++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("RelativeETIndex(").Append(i.ToString()).Append(")(").Append(k.ToString()).Append("\n");
                    for (k = 0; k < landUseClasses; k++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("MaxCapacity(").Append(i.ToString()).Append(")(").Append(k.ToString()).Append("\n");
                    for (k = 0; k < landUseClasses; k++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("InundationThreshold(").Append(i.ToString()).Append(")(").Append(k.ToString()).Append("\n");
                    for (k = 0; k < landUseClasses; k++) header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("Porosity(").Append(i.ToString()).Append(")(").Append(k.ToString()).Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("IsQuickBox(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("AllowBidirectionalFlow(").Append(i.ToString()).Append(")").Append("\n");
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("UseForSMDCalculation(").Append(i.ToString()).Append(")").Append("\n");
                }
                for (j = 0; j < landUseClasses; j++)
                {
                    header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("LandCoverNumber(").Append(i.ToString()).Append(")").Append("\n");
                    for (l = 0; l < numberOfBoxes; l++)
                    {
                        for (n = 0; n < numberOfBoxes; n++)
                        {
                            header.Append((m++).ToString()).Append(MCParameters.separatorChar).Append("SM(").Append(j.ToString()).Append(")(").Append(l.ToString()).Append(")(").Append(m.ToString()).Append(")").Append("\n");
                        }
                    }
                }
            }
        }
    }
