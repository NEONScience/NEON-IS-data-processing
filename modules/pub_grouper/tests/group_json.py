import json

def get_data_product() -> str:
    return 'DP1.00066.001'

def get_group_json() -> str:
    """Returns the group JSON to use when testing."""
    group_json = {
        'type': 'FeatureCollection',
        'features': [
            {
                'type': 'Feature',
                'geometry': None,
                'properties': {
                    'name': 'CFGLOC108605',
                    'group': 'par-quantum-line_CPER001000',
                    'active_periods': [
                        {
                            'start_date': '2017-07-20T00:00:00Z'
                        }
                    ],
                    'data_product_ID': [
                        get_data_product()
                    ]
                },
                'site': 'CPER',
                'domain': 'D10',
                'visibility_code': 'public',
                'HOR': '001',
                'VER': '000'
            }
        ]
    }
    return json.dumps(group_json)
