import json
import logging
import urllib.request

_LOGGER = logging.getLogger(__name__)

URL_POWER_FLOW = "{}://{}/solar_api/v1/GetPowerFlowRealtimeData.fcgi"
URL_SYSTEM_METER = "{}://{}/solar_api/v1/GetMeterRealtimeData.cgi?Scope=System"
URL_SYSTEM_INVERTER = "{}://{}/solar_api/v1/GetInverterRealtimeData.cgi?Scope=System"
URL_DEVICE_METER = "{}://{}/solar_api/v1/GetMeterRealtimeData.cgi?Scope=Device&DeviceId={}"
URL_DEVICE_STORAGE = "{}://{}/solar_api/v1/GetStorageRealtimeData.cgi?Scope=Device&DeviceId={}"
URL_DEVICE_INVERTER_CUMULATIVE = "{}://{}/solar_api/v1/GetInverterRealtimeData.cgi?Scope=Device&DeviceId={}&DataCollection=CumulationInverterData"
URL_DEVICE_INVERTER_COMMON = "{}://{}/solar_api/v1/GetInverterRealtimeData.cgi?Scope=Device&DeviceId={}&DataCollection=CommonInverterData"

class Fronius:
    '''
    Interface to communicate with the Fronius Symo over http / JSON
    Attributes:
        host        The ip/domain of the Fronius device
        useHTTPS    Use HTTPS instead of HTTP
        timeout     HTTP timeout in seconds
    '''
    def __init__(self, host, useHTTPS = False, timeout = 10):
        '''
        Constructor
        '''
        self.host = host
        self.timeout = timeout
        if useHTTPS:
            self.protocol = "https"
        else:
            self.protocol = "http"


    def current_power_flow(self):
        '''
        Get the current power flow of a smart meter system.
        '''
        url = URL_POWER_FLOW.format(self.protocol, self.host)

        _LOGGER.debug("Get current system power flow data for {}".format(url))

        return self._current_data(url, self._system_power_flow)


    def current_system_meter_data(self):
        '''
        Get the current meter data.
        '''
        url = URL_SYSTEM_METER.format(self.protocol, self.host)

        _LOGGER.debug("Get current system meter data for {}".format(url))

        return self._current_data(url, self._system_meter_data)


    def current_system_inverter_data(self):
        '''
        Get the current inverter data.
        The values are provided as cumulated values and for each inverter
        '''
        url = URL_SYSTEM_INVERTER.format(self.protocol, self.host)

        _LOGGER.debug("Get current system inverter data for {}".format(url))

        return self._current_data(url, self._system_inverter_data)


    def current_meter_data(self, device = 0):
        '''
        Get the current meter data for a device.
        '''
        url = URL_DEVICE_METER.format(self.protocol, self.host, device)

        _LOGGER.debug("Get current meter data for {}".format(url))

        return self._current_data(url, self._device_meter_data)


    def current_storage_data(self, device = 0):
        '''
        Get the current storage data for a device.
        '''
        url = URL_DEVICE_STORAGE.format(self.protocol, self.host, device)

        _LOGGER.debug("Get current storage data for {}".format(url))

        return self._current_data(url, self._device_storage_data)


    def current_inverter_data(self, device = 1):
        '''
        Get the current inverter data of one device.
        '''
        url = URL_DEVICE_INVERTER_COMMON.format(self.protocol, self.host, device)

        _LOGGER.debug("Get current inverter data for {}".format(url))

        return self._current_data(url, self._device_inverter_data)


    def _fetch_json(self, url):
        _LOGGER.info("Fetch data from {}".format(url))
        request = urllib.request.urlopen(url)
        return json.loads(request.read().decode())


    def _status_data(self, json):

        sensor = {}

        sensor['timestamp'] = { 'value': json['Head']['Timestamp'] }
        sensor['status'] = json['Head']['Status']
        sensor['status_code'] = { 'value': json['Head']['Status']['Code'] }
        sensor['status_reason'] = { 'value': json['Head']['Status']['Reason'] }
        sensor['status_message'] = { 'value': json['Head']['Status']['UserMessage'] }

        return sensor


    def _current_data(self, url, fun):
        json = self._fetch_json(url)
        sensor = self._status_data(json)

        # break if Data is empty
        if not json['Body'] or not json['Body']['Data']:
            _LOGGER.info("No data returned from {}".format(url))
            return sensor
        else:
            return fun(sensor, json['Body']['Data'])


    def _system_power_flow(self, sensor, data):
        _LOGGER.debug("Converting system power flow data: '{}'".format(data))

        site = data['Site']
        inverter = data['Inverters']['1'] # TODO: implement more inverters
  
        self._copy(inverter, sensor, "Battery_Mode", 'battery_mode')
        self._copy(inverter, sensor, "SOC", 'state_of_charge', '%')

        self._copy(site, sensor, "BatteryStandby", 'battery_standby')
        self._copy(site, sensor, "E_Day", 'energy_day', 'Wh')
        self._copy(site, sensor, "E_Total", 'energy_total', 'Wh')
        self._copy(site, sensor, "E_Year", 'energy_year', 'Wh')
        self._copy(site, sensor, "Meter_Location", 'meter_location')
        self._copy(site, sensor, "Mode", 'meter_mode')
        self._copy(site, sensor, "P_Akku", 'power_battery', 'W')
        self._copy(site, sensor, "P_Grid", 'power_grid', 'W')
        self._copy(site, sensor, "P_Load", 'power_load', 'W')
        self._copy(site, sensor, "P_PV", 'power_photovoltaics', 'W')
        self._copy(site, sensor, "rel_Autonomy", 'relative_autonomy', '%')
        self._copy(site, sensor, "rel_SelfConsumption", 'relative_self_consumption', '%')

        return sensor


    def _system_meter_data(self, sensor, data):
        _LOGGER.debug("Converting system meter data: '{}'".format(data))

        sensor['meters'] = { }

        for i in data:
            sensor['meters'][i] = self._meter_data(data[i])

        return sensor


    def _system_inverter_data(self, sensor, data):
        _LOGGER.debug("Converting system inverter data: '{}'".format(data))

        sensor['energy_day'] = { 'value': 0, 'unit': "Wh" }
        sensor['energy_total'] = { 'value': 0, 'unit': "Wh" }
        sensor['energy_year'] = { 'value': 0, 'unit': "Wh" }
        sensor['power_ac'] = { 'value': 0, 'unit': "W" }

        sensor['inverters'] = {}

        if "DAY_ENERGY" in data:
            for i in data['DAY_ENERGY']['Values']:
                sensor['inverters'][i] = { }
                sensor['inverters'][i]['energy_day'] = { 'value': data['DAY_ENERGY']['Values'][i], 'unit': data['DAY_ENERGY']['Unit'] }
                sensor['energy_day']['value'] += data['DAY_ENERGY']['Values'][i]
        if "TOTAL_ENERGY" in data:
            for i in data['TOTAL_ENERGY']['Values']:
                sensor['inverters'][i]['energy_total'] = { 'value': data['TOTAL_ENERGY']['Values'][i], 'unit': data['TOTAL_ENERGY']['Unit'] }
                sensor['energy_total']['value'] += data['TOTAL_ENERGY']['Values'][i]
        if "YEAR_ENERGY" in data:
            for i in data['YEAR_ENERGY']['Values']:
                sensor['inverters'][i]['energy_year'] = { 'value': data['YEAR_ENERGY']['Values'][i], 'unit': data['TOTAL_ENERGY']['Unit'] }
                sensor['energy_year']['value'] += data['YEAR_ENERGY']['Values'][i]
        if "PAC" in data:
            for i in data['PAC']['Values']:
                sensor['inverters'][i]['power_ac'] = { 'value': data['PAC']['Values'][i], 'unit': data['TOTAL_ENERGY']['Unit'] }
                sensor['power_ac']['value'] += data['PAC']['Values'][i]

        return sensor


    def _device_meter_data(self, sensor, data):
        _LOGGER.debug("Converting meter data: '{}'".format(data))

        sensor.update(self._meter_data(data))

        return sensor


    def _device_storage_data(self, sensor, data):
        _LOGGER.debug("Converting storage data from '{}'".format(data))

        if 'Controller' in data:
            controller = self._controller_data(data['Controller'])
            sensor.update(controller)

        if 'Modules' in data:
            sensor['modules'] = { }
            module_count = 0;

            for module in data['Modules']:
                sensor['modules'][module_count] = self._module_data(module)
                module_count += 1

        return sensor


    def _device_inverter_data(self, sensor, data):
        _LOGGER.debug("Converting inverter data from '{}'".format(data))

        self._copy(data, sensor, "DAY_ENERGY", 'energy_day')
        self._copy(data, sensor, "TOTAL_ENERGY", 'energy_total')
        self._copy(data, sensor, "YEAR_ENERGY", 'energy_year')
        self._copy(data, sensor, "FAC", 'frequency_ac')
        self._copy(data, sensor, "IAC", 'current_ac')
        self._copy(data, sensor, "IDC", 'current_dc')
        self._copy(data, sensor, "PAC", 'power_ac')
        self._copy(data, sensor, "UAC", 'voltage_ac')
        self._copy(data, sensor, "UDC", 'voltage_dc')

        return sensor


    def _meter_data(self, data):

        meter = {}

        self._copy(data, meter, "Current_AC_Phase_1", 'current_ac_phase_1', 'A')
        self._copy(data, meter, "Current_AC_Phase_2", 'current_ac_phase_2', 'A')
        self._copy(data, meter, "Current_AC_Phase_3", 'current_ac_phase_3', 'A')
        self._copy(data, meter, "EnergyReactive_VArAC_Sum_Consumed", 'energy_reactive_ac_consumed', 'Wh')
        self._copy(data, meter, "EnergyReactive_VArAC_Sum_Produced", 'energy_reactive_ac_produced', 'Wh')
        self._copy(data, meter, "EnergyReal_WAC_Minus_Absolute", 'energy_real_ac_minus', 'Wh')
        self._copy(data, meter, "EnergyReal_WAC_Plus_Absolute", 'energy_real_ac_plus', 'Wh')
        self._copy(data, meter, "EnergyReal_WAC_Sum_Consumed", 'energy_real_consumed', 'Wh')
        self._copy(data, meter, "EnergyReal_WAC_Sum_Produced", 'energy_real_produced', 'Wh')
        self._copy(data, meter, "Frequency_Phase_Average", 'frequency_phase_average', 'H')
        self._copy(data, meter, "PowerApparent_S_Phase_1", 'power_apparent_phase_1', 'W')
        self._copy(data, meter, "PowerApparent_S_Phase_2", 'power_apparent_phase_2', 'W')
        self._copy(data, meter, "PowerApparent_S_Phase_3", 'power_apparent_phase_3', 'W')
        self._copy(data, meter, "PowerApparent_S_Sum", 'power_apparent', 'W')
        self._copy(data, meter, "PowerFactor_Phase_1", 'power_factor_phase_1', 'W')
        self._copy(data, meter, "PowerFactor_Phase_2", 'power_factor_phase_2', 'W')
        self._copy(data, meter, "PowerFactor_Phase_3", 'power_factor_phase_3', 'W')
        self._copy(data, meter, "PowerFactor_Sum", 'power_factor', 'W')
        self._copy(data, meter, "PowerReactive_Q_Phase_1", 'power_reactive_phase_1', 'W')
        self._copy(data, meter, "PowerReactive_Q_Phase_2", 'power_reactive_phase_2', 'W')
        self._copy(data, meter, "PowerReactive_Q_Phase_3", 'power_reactive_phase_3', 'W')
        self._copy(data, meter, "PowerReactive_Q_Sum", 'power_reactive', 'W')
        self._copy(data, meter, "PowerReal_P_Phase_1", 'power_real_phase_1', 'W')
        self._copy(data, meter, "PowerReal_P_Phase_2", 'power_real_phase_2', 'W')
        self._copy(data, meter, "PowerReal_P_Phase_3", 'power_real_phase_3', 'W')
        self._copy(data, meter, "PowerReal_P_Sum", 'power_real', 'W')
        self._copy(data, meter, "Voltage_AC_Phase_1", 'voltage_ac_phase_1', 'V')
        self._copy(data, meter, "Voltage_AC_Phase_2", 'voltage_ac_phase_2', 'V')
        self._copy(data, meter, "Voltage_AC_Phase_3", 'voltage_ac_phase_3', 'V')
        self._copy(data, meter, "Voltage_AC_PhaseToPhase_12", 'voltage_ac_phase_to_phase_12', 'V')
        self._copy(data, meter, "Voltage_AC_PhaseToPhase_23", 'voltage_ac_phase_to_phase_23', 'V')
        self._copy(data, meter, "Voltage_AC_PhaseToPhase_31", 'voltage_ac_phase_to_phase_31', 'V')

        self._copy(data, meter, "Meter_Location_Current", 'meter_location')
        self._copy(data, meter, "Enable", 'enable')
        self._copy(data, meter, "Visible", 'visible')
        if "Details" in data:
            self._copy(data['Details'], meter, "Manufacturer", 'manufacturer')
            self._copy(data['Details'], meter, "Model", 'model')
            self._copy(data['Details'], meter, "Serial", 'serial')

        return meter


    def _controller_data(self, data):

        controller = {}

        self._copy(data, controller, "Capacity_Maximum", 'capacity_maximum', 'Ah')
        self._copy(data, controller, "DesignedCapacity", 'capacity_designed', 'Ah')
        self._copy(data, controller, "Current_DC", 'current_dc', 'A')
        self._copy(data, controller, "Voltage_DC", 'voltage_dc', 'V')
        self._copy(data, controller, "Voltage_DC_Maximum_Cell", 'voltage_dc_maximum_cell', 'V')
        self._copy(data, controller, "Voltage_DC_Minimum_Cell", 'voltage_dc_minimum_cell', 'V')
        self._copy(data, controller, "StateOfCharge_Relative", 'state_of_charge', '%')
        self._copy(data, controller, "Temperature_Cell", 'temperature_cell', 'C')
        self._copy(data, controller, "Enable", 'enable')
        if "Details" in data:
            self._copy(data['Details'], controller, "Manufacturer", 'manufacturer')
            self._copy(data['Details'], controller, "Model", 'model')
            self._copy(data['Details'], controller, "Serial", 'serial')

        return controller


    def _module_data(self, data):

        module = { }


        self._copy(data, module, "Capacity_Maximum", 'capacity_maximum', 'Ah')
        self._copy(data, module, "DesignedCapacity", 'capacity_designed', 'Ah')
        self._copy(data, module, "Current_DC", 'current_dc', 'A')
        self._copy(data, module, "Voltage_DC", 'voltage_dc', 'V')
        self._copy(data, module, "Voltage_DC_Maximum_Cell", 'voltage_dc_maximum_cell', 'V')
        self._copy(data, module, "Voltage_DC_Minimum_Cell", 'voltage_dc_minimum_cell', 'V')
        self._copy(data, module, "StateOfCharge_Relative", 'state_of_charge', '%')
        self._copy(data, module, "Temperature_Cell", 'temperature_cell', 'C')
        self._copy(data, module, "Temperature_Cell_Maximum", 'temperature_cell_maximum', 'C')
        self._copy(data, module, "Temperature_Cell_Minimum", 'temperature_cell_minimum', 'C')
        self._copy(data, module, "CycleCount_BatteryCell", 'cycle_count_cell', 'C')
        self._copy(data, module, "Status_BatteryCell", 'status_cell')

        self._copy(data, module, "Enable", 'enable')
        if "Details" in data:
            self._copy(data['Details'], module, "Manufacturer", 'manufacturer')
            self._copy(data['Details'], module, "Model", 'model')
            self._copy(data['Details'], module, "Serial", 'serial')

        return module

    def _copy(self, source, target, sid, tid, unit = None):
    	
        if sid in source and isinstance(source[sid], dict) and 'Value' in source[sid]:
            target[tid] = { 'value': source[sid]['Value'] }
            if "Unit" in source[sid]:
                target[tid]['unit'] = source[sid]['Unit'] 
        elif sid in source:
            target[tid] = { 'value': source[sid] }
        else:
            target[tid] = { 'value': 0 }

        if unit is not None:
            target[tid]['unit'] = unit 

        return
