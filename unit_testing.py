import unittest
from influxdb import InfluxDBClient


class TestStringMethods(unittest.TestCase):
    def __init__(self):
        super().__init__()
        self.client = InfluxDBClient('me-hammer10.engin.umich.edu', 8086,
                                     'battmaster', 'allmydata999')

    def test_arbin_voltage(self):
        self.client.switch_database('testing')
        results = self.client.query(
            "select value from UM34 where original_file_name='UM34_Test005E.res' and specific_measurement_type='Voltage(V)'")
        points = list(results.get_points())
        exp = [{'time': '2020-03-06T12:54:57Z',
                'value': 2.84494972229003},
               {'time': '2020-03-06T12:55:07Z',
                'value': 2.84494972229003},
               {'time': '2020-03-06T12:55:17Z',
                'value': 2.89658045768737},
               {'time': '2020-03-06T12:55:27Z',
                'value': 2.9196298122406},
               {'time': '2020-03-06T12:55:37Z',
                'value': 2.93530344963073}
               ]
        self.assertEqual(points[4:9], exp)

    def test_biologic_Ecell(self):
        self.client.switch_database('testing')
        results = self.client.query(
            "select value from UM26 where original_file_name='UM26_Test001E_CA8.mpr' and specific_measurement_type='Ecell/V'")
        points = list(results.get_points())
        exp = [
            {'time': '2020-02-26T10:35:29Z',
             'value': 0.000000000000000000e+00},
        ]
        self.assertEqual(points[0], exp)

    def test_quickdaq_current(self):
        self.client.switch_database('testing')
        results = self.client.query(
            "select value from UM34 where original_file_name='QuickDAQF_UM34_Test005E.hpf' and specific_measurement_type='Current/A'")
        points = list(results.get_points())
        exp = [{'time': '2020-03-06T12:53:50.160372Z',
                'value': 4.683645784098189e-06},
               {'time': '2020-03-06T12:53:51.160372Z',
                'value': 4.683645784098189e-06},
               {'time': '2020-03-06T12:53:52.160372Z',
                'value': 4.8546039579377975e-06},
               {'time': '2020-03-06T12:53:53.160372Z',
                'value': 4.879026619164506e-06},
               {'time': '2020-03-06T12:53:54.160372Z',
                'value': 4.8546039579377975e-06}
               ]
        self.assertEqual(points[:5], exp)

    def test_tdms_disp(self):
        self.client.switch_database('testing')
        results = self.client.query(
            "select value from UM34 where original_file_name='UM34_Test005E.tdms'")
        points = list(results.get_points())
        exp = {'time': '2020-03-06T12:53:50.160372Z',
               'value': 0.000000}
        self.assertEqual(points[0], exp)


if __name__ == '__main__':
    unittest.main()
