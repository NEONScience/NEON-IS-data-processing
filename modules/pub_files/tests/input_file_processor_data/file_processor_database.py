from pub_files.data_product import DataProduct, build_data_product
from pub_files.input_files.file_processor_database import FileProcessorDatabase
from pub_files.tests.publication_workbook.publication_workbook import get_workbook


def get_data_product(_data_product_id: str) -> DataProduct:
    """Mock function for reading the data product."""
    return build_data_product(data_product_id='NEON.DOM.SITE.DP1.00041.001',
                              name='Soil temperature',
                              type_name='TIS Data Product Type',
                              description='Temperature of the soil at various depth below the soil surface from 2 cm up to 200 cm at non-permafrost sites (up to 300 cm at Alaskan sites). Data are from all five Instrumented Soil Plots per site and presented as 1-minute and 30-minute averages.',
                              category='Level 1 Data Product',
                              supplier='TIS',
                              short_name='temp-soil',
                              abstract='Soil temperature is measured at various depths below the soil surface from approximately 2 cm up to 200 cm at non-permafrost sites (up to 300 cm at Alaskan sites). Soil temperature influences the rate of biogeochemical cycling, decomposition, and root and soil biota activity. In addition, soil temperature can impact the hydrologic cycle since it controls whether soil water is in a liquid or solid state. Measurements are made in vertical profiles consisting of up to nine depths in all five instrumented soil plots at each terrestrial site, and presented as 1-minute and 30-minute averages.\n\nLatency:\nData collected in any given month are published during the second full week of the following month.',
                              design_description='When possible the soil plots were arranged in a transect with the first plot approximately 15-40 m from the tower in the expected dominant airshed. The middle of airshed was used as the transect vector and plot spacing was based on the distance required for surface soil temperature and moisture measurements to be spatially independent at the 1 hectare scale during site characterization (capped at approximately 40 m due to logistical constraints). Soil plots were microsited as necessary to avoid obstacles (e.g., boulders, streams, and paths) and more compact plot layouts were used at small sites. Soil temperature is measured at up to nine depths within each plot, with the mid-point of the shallowest sensors at approximately 2, 6, 16, and 26 cm. Depths for deeper sensors vary among sites and are based on megapit soil horizon data (NEON.DP1.00097) and depth to restrictive feature (see NEON.DOC.003146).',
                              study_description='Soil temperature is measured in all five instrumented soil plots at each terrestrial site. Sensor depths can be found in the \"…sensor_positions…\" file in the data product download package. zOffset represents sensor depth in meters relative to the soil surface (negative numbers indicate sensor is below the soil surface). Each row corresponds to a different temperature sensor installed at the site as indicated in the HOR.VER column. For example, 2.503 in the HOR.VER column refers to the sensor in soil plot 2 measurement level 3, while 4.508 refers to soil plot 4 measurement level 8).',
                              sensor='Thermometrics - Climate RTD 100-ohm Probe',
                              basic_description='Includes the data product, summary statistics, expanded uncertainty, and final quality flag.',
                              expanded_description='Includes the basic package information plus quality metrics for all of the quality assessment and quality control analyses.',
                              remarks='Remarks.')


def get_file_processor_database() -> FileProcessorDatabase:
    return FileProcessorDatabase(get_data_product=get_data_product, get_workbook=get_workbook)
