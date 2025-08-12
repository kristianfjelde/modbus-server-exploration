#!/usr/bin/env python3
"""
Simple Modbus TCP Server for Brewery Simulator
"""
import asyncio
import logging
from pymodbus import ModbusDeviceIdentification
from pymodbus.datastore import ModbusSequentialDataBlock, ModbusDeviceContext, ModbusServerContext
from pymodbus.server import StartAsyncTcpServer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BreweryModbusServer:
    def __init__(self, host='0.0.0.0', port=502):
        self.host = host
        self.port = port
        self.server_task = None

        # Register ranges
        self.CHILLER_BASE = 30001
        self.FERMENTER_BASE = 30021
        self.REGISTERS_PER_FERMENTER = 10

        # Active fermenters (can be managed dynamically)
        self.fermenters = {}  # {fermenter_id: fermenter_index}

        # Setup Modbus context
        self._setup_datastore()

    def _setup_datastore(self):
        """Create the Modbus data store"""
        # Create data blocks
        # We need large enough blocks to handle multiple fermenters
        MAX_REGISTERS = 1000

        # Input registers (function 4) - for sensor readings (read-only)
        input_registers = ModbusSequentialDataBlock(30001, [0] * MAX_REGISTERS)

        # Holding registers (function 3) - for setpoints (read/write)
        holding_registers = ModbusSequentialDataBlock(40001, [0] * MAX_REGISTERS)

        # We don't need coils or discrete inputs for this application
        coils = ModbusSequentialDataBlock(1, [False] * 100)
        discrete_inputs = ModbusSequentialDataBlock(1, [False] * 100)

        # Create device context
        device_context = ModbusDeviceContext(
            di=discrete_inputs,  # Discrete inputs (not used)
            co=coils,  # Coils (not used)
            hr=holding_registers,  # Holding registers (setpoints)
            ir=input_registers  # Input registers (sensor data)
        )

        # Create server context (single device, address 0)
        self.context = ModbusServerContext(devices=device_context, single=True)

    def add_fermenter(self, fermenter_id):
        """Add a fermenter and assign it a register block"""
        if fermenter_id in self.fermenters:
            logger.warning(f"Fermenter {fermenter_id} already exists")
            return

        # Find next available fermenter index
        used_indices = set(self.fermenters.values())
        fermenter_index = 0
        while fermenter_index in used_indices:
            fermenter_index += 1

        self.fermenters[fermenter_id] = fermenter_index
        logger.info(f"Added fermenter {fermenter_id} at index {fermenter_index}")

        # Initialize fermenter registers to sensible defaults
        base_addr = self.FERMENTER_BASE + (fermenter_index * self.REGISTERS_PER_FERMENTER)
        initial_values = [
            200,  # current_temp: 20.0°C
            200,  # setpoint: 20.0°C
            20,  # supply_temp: 2.0°C
            80,  # return_temp: 8.0°C
            0,  # cooling_active: off
            0,  # duty_cycle: 0%
            0,  # heat_load: 0W
            0,  # fermentation_heat: 0W
            0,  # alarm_status: no alarms
            1  # status: OK
        ]

        # Update input registers
        slave_context = self.context[0]
        for i, value in enumerate(initial_values):
            slave_context.setValues(4, base_addr + i, [value])

        return fermenter_index

    def remove_fermenter(self, fermenter_id):
        """Remove a fermenter"""
        if fermenter_id not in self.fermenters:
            logger.warning(f"Fermenter {fermenter_id} not found")
            return

        fermenter_index = self.fermenters.pop(fermenter_id)

        # Clear the registers
        base_addr = self.FERMENTER_BASE + (fermenter_index * self.REGISTERS_PER_FERMENTER)
        slave_context = self.context[0]
        for i in range(self.REGISTERS_PER_FERMENTER):
            slave_context.setValues(4, base_addr + i, [0])

        logger.info(f"Removed fermenter {fermenter_id}")

    def update_chiller_data(self, data):
        """Update chiller system registers"""
        slave_context = self.context[0]

        # Map data to registers (temperatures scaled by 10)
        registers = {
            self.CHILLER_BASE + 0: int(data.get('reservoir_temp', 0) * 10),
            self.CHILLER_BASE + 1: int(data.get('supply_temp', 0) * 10),
            self.CHILLER_BASE + 2: int(data.get('return_temp', 0) * 10),
            self.CHILLER_BASE + 3: 1 if data.get('compressor_running', False) else 0,
            self.CHILLER_BASE + 4: int(data.get('compressor_power', 0)),
            self.CHILLER_BASE + 5: int(data.get('total_heat_load', 0)),
            self.CHILLER_BASE + 6: int(data.get('setpoint', 0) * 10),
            self.CHILLER_BASE + 7: int(data.get('efficiency', 0) * 10),
            self.CHILLER_BASE + 8: data.get('alarm_status', 0),
            self.CHILLER_BASE + 9: data.get('system_status', 1),
        }

        # Update input registers (read-only sensor data)
        for address, value in registers.items():
            slave_context.setValues(4, address, [value])

        # Also update setpoint in holding registers (writable)
        slave_context.setValues(3, 40001, [registers[self.CHILLER_BASE + 6]])

    def update_fermenter_data(self, fermenter_id, data):
        """Update fermenter registers"""
        if fermenter_id not in self.fermenters:
            logger.warning(f"Fermenter {fermenter_id} not found, adding it")
            self.add_fermenter(fermenter_id)

        fermenter_index = self.fermenters[fermenter_id]
        base_addr = self.FERMENTER_BASE + (fermenter_index * self.REGISTERS_PER_FERMENTER)

        slave_context = self.context[0]

        # Map data to registers
        registers = [
            int(data.get('current_temp', 0) * 10),  # +0
            int(data.get('setpoint', 0) * 10),  # +1
            int(data.get('supply_temp', 0) * 10),  # +2
            int(data.get('return_temp', 0) * 10),  # +3
            1 if data.get('cooling_active', False) else 0,  # +4
            int(data.get('duty_cycle', 0) * 100),  # +5 (percentage * 100)
            int(data.get('heat_load_to_chiller', 0)),  # +6
            int(data.get('fermentation_heat', 0)),  # +7
            data.get('alarm_status', 0),  # +8
            data.get('status', 1)  # +9
        ]

        # Update input registers
        for i, value in enumerate(registers):
            slave_context.setValues(4, base_addr + i, [value])

        # Also update setpoint in holding registers (writable)
        slave_context.setValues(3, 40001 + fermenter_index, [registers[1]])

    def read_setpoints(self):
        """Read any setpoints that may have been written by the gateway"""
        slave_context = self.context[0]
        setpoints = {}

        # Read chiller setpoint from holding registers
        chiller_setpoint = slave_context.getValues(3, 40001, 1)[0]
        setpoints['chiller_setpoint'] = chiller_setpoint / 10.0

        # Read fermenter setpoints
        fermenter_setpoints = {}
        for fermenter_id, fermenter_index in self.fermenters.items():
            setpoint_raw = slave_context.getValues(3, 40001 + fermenter_index, 1)[0]
            fermenter_setpoints[fermenter_id] = setpoint_raw / 10.0

        setpoints['fermenters'] = fermenter_setpoints
        return setpoints

    async def start_server(self):
        """Start the Modbus TCP server"""
        # Device identification
        identity = ModbusDeviceIdentification(
            info_name={
                "VendorName": "Brewery Simulator",
                "ProductCode": "BS-001",
                "VendorUrl": "https://github.com/brewery-simulator",
                "ProductName": "Virtual Brewery System",
                "ModelName": "Brewery Cooling Simulator",
                "MajorMinorRevision": "1.0.0",
            }
        )

        logger.info(f"Starting Modbus TCP server on {self.host}:{self.port}")

        # Start server - StartAsyncTcpServer should be awaited directly, not in a task
        try:
            await StartAsyncTcpServer(
                context=self.context,
                identity=identity,
                address=(self.host, self.port),
            )

        except Exception as e:
            logger.error(f"Failed to start server: {e}")
            raise

    async def stop_server(self):
        """Stop the Modbus server"""
        # Since we're awaiting StartAsyncTcpServer directly,
        # stopping is handled by KeyboardInterrupt in the main loop
        logger.info("Modbus TCP server stopped")

    def list_fermenters(self):
        """Get list of active fermenters"""
        return list(self.fermenters.keys())

    def get_register_map(self):
        """Get current register mapping for debugging"""
        reg_map = {
            "chiller": {
                "reservoir_temp": self.CHILLER_BASE + 0,
                "supply_temp": self.CHILLER_BASE + 1,
                "return_temp": self.CHILLER_BASE + 2,
                "compressor_running": self.CHILLER_BASE + 3,
                "compressor_power": self.CHILLER_BASE + 4,
                "total_heat_load": self.CHILLER_BASE + 5,
                "setpoint": self.CHILLER_BASE + 6,
                "efficiency": self.CHILLER_BASE + 7,
                "alarm_status": self.CHILLER_BASE + 8,
                "system_status": self.CHILLER_BASE + 9,
            },
            "fermenters": {}
        }

        for fermenter_id, fermenter_index in self.fermenters.items():
            base = self.FERMENTER_BASE + (fermenter_index * self.REGISTERS_PER_FERMENTER)
            reg_map["fermenters"][fermenter_id] = {
                "base_address": base,
                "current_temp": base + 0,
                "setpoint": base + 1,
                "supply_temp": base + 2,
                "return_temp": base + 3,
                "cooling_active": base + 4,
                "duty_cycle": base + 5,
                "heat_load": base + 6,
                "fermentation_heat": base + 7,
                "alarm_status": base + 8,
                "status": base + 9,
            }

        return reg_map

    def get_network_info(self):
        """Get network interface information for debugging"""
        import socket
        import subprocess

        info = {}

        # Get hostname and IP
        try:
            hostname = socket.gethostname()
            local_ip = socket.gethostbyname(hostname)
            info['hostname'] = hostname
            info['local_ip'] = local_ip
        except Exception as e:
            info['hostname_error'] = str(e)

        # Get all network interfaces (Linux)
        try:
            result = subprocess.run(['hostname', '-I'], capture_output=True, text=True)
            if result.returncode == 0:
                info['all_ips'] = result.stdout.strip().split()
        except Exception as e:
            info['ip_error'] = str(e)

        # Check if port 502 is in use
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1)
                result = s.connect_ex(('localhost', 502))
                info['port_502_local'] = "OPEN" if result == 0 else "CLOSED"
        except Exception as e:
            info['port_check_error'] = str(e)

        return info


# Simple test/demo script
async def simulation_task(server):
    """Run the simulation loop"""
    import math
    import time

    # Give server a moment to start
    await asyncio.sleep(1)

    # Add some fermenters
    server.add_fermenter("FV001")
    server.add_fermenter("FV002")

    # Print register map
    print("\n=== Register Map ===")
    reg_map = server.get_register_map()

    print("Chiller registers:")
    for name, addr in reg_map["chiller"].items():
        print(f"  {name}: {addr}")

    print("\nFermenter registers:")
    for fv_id, fv_regs in reg_map["fermenters"].items():
        print(f"  {fv_id} (base {fv_regs['base_address']}):")
        for name, addr in fv_regs.items():
            if name != "base_address":
                print(f"    {name}: {addr}")

    print(f"\nServer running on {server.host}:{server.port}")
    print("Configure your gateway to read these registers")
    print("Press Ctrl+C to stop\n")

    # Simulation loop
    start_time = time.time()
    while True:
        elapsed = time.time() - start_time

        # Update chiller data
        chiller_data = {
            'reservoir_temp': 2.0 + 0.5 * math.sin(elapsed / 60),
            'supply_temp': 2.0 + 0.3 * math.sin(elapsed / 60),
            'return_temp': 8.0 + 2.0 * math.sin(elapsed / 120),
            'compressor_running': (elapsed % 180) < 120,
            'compressor_power': 4500 if (elapsed % 180) < 120 else 0,
            'total_heat_load': 12000 + 3000 * math.sin(elapsed / 90),
            'setpoint': 2.0,
            'efficiency': 85.0,
            'alarm_status': 0,
            'system_status': 1
        }
        server.update_chiller_data(chiller_data)

        # Update fermenter data
        for i, fv_id in enumerate(["FV001", "FV002"]):
            fermenter_data = {
                'current_temp': 18.0 + i + 1.0 * math.sin(elapsed / 200 + i),
                'setpoint': 18.0 + i,
                'supply_temp': chiller_data['supply_temp'],
                'return_temp': chiller_data['return_temp'] - 1.0,
                'cooling_active': chiller_data['compressor_running'],
                'duty_cycle': 0.75 + 0.2 * math.sin(elapsed / 150 + i),
                'heat_load_to_chiller': 3000 + 1000 * math.sin(elapsed / 100 + i),
                'fermentation_heat': 500 + 200 * math.sin(elapsed / 400 + i),
                'alarm_status': 0,
                'status': 1
            }
            server.update_fermenter_data(fv_id, fermenter_data)

        # Print status
        temps = [f"{fv}: {18.0 + i + 1.0 * math.sin(elapsed / 200 + i):.1f}°C"
                 for i, fv in enumerate(["FV001", "FV002"])]
        print(f"\rChiller: {chiller_data['reservoir_temp']:.1f}°C | {' | '.join(temps)}", end="")

        await asyncio.sleep(5)


async def main():
    """Demo the Modbus server"""
    # Create server
    server = BreweryModbusServer()

    # Show network info for debugging
    print("\n=== Network Information ===")
    net_info = server.get_network_info()
    for key, value in net_info.items():
        print(f"{key}: {value}")

    try:
        # Run server and simulation concurrently
        await asyncio.gather(
            server.start_server(),
            simulation_task(server)
        )
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        await server.stop_server()


if __name__ == "__main__":
    asyncio.run(main())