from maintenance_table_loader.table_loader import Table


def get_tables() -> list[Table]:
    """Mock function to return a maintenance table."""
    return [Table(id=1550,
                  data_product='NEON.DOM.SITE.DP1.00026.001',
                  source_data_product='NEON.DOM.SITE.DP1.00026.001',
                  name='ais_maintenanceGroundwater_pub',
                  description='Information related to groundwater sensor and infrastructure maintenance',
                  usage='both',
                  table_type='site-date',
                  ingest_table_id=1107,
                  filter_sample_class=None)]
