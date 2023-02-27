#!/usr/bin/env python3

def get_location_data():
    return {
        'type': 'FeatureCollection',
        'features': [
            {
                'type': 'Feature',
                'geometry': None,
                'properties': {
                    'name': 'CFGLOC101746',
                    'type': 'CONFIG',
                    'description': 'Central Plains Soil Temp Profile SP1, Z5 Depth',
                    'domain': 'D10',
                    'site': 'CPER',
                    'context': ['soil'],
                    'active_periods': [
                        {
                            'start_date': '2016-04-08T00:00:00Z'
                        }
                    ]
                },
                'HOR': '001',
                'VER': '505',
                'TMI': '000',
                'Data Rate': '0.1',
                'Required Asset Management Location ID': 3095,
                'Required Asset Management Location Code': 'CFGLOC101746'
            }
        ]
    }
