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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DebuggingModbusDeviceContext(ModbusDeviceContext):
    """Custom device context that logs all incoming requests"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.request_count = 0

    def getValues(self, fx, address, count=1):
        """Override to log all incoming read requests"""
        self.request_count += 1

        # Map function codes to names
        fx_names = {
            1: "Read Coils",
            2: "Read Discrete Inputs",
            3: "Read Holding Registers",
            4: "Read Input Registers"
        }

        fx_name = fx_names.get(fx, f"Unknown Function {fx}")

        # Log the incoming request
        logger.info("=" * 60)
        logger.info(f"📥 INCOMING REQUEST #{self.request_count}")
        logger.info(f"   Function Code: {fx} ({fx_name})")
        logger.info(f"   Start Address: {address}")
        logger.info(f"   Register Count: {count}")
        logger.info(f"   Address Range: {address} to {address + count - 1}")

        # Get the actual values from the datastore
        try:
            values = super().getValues(fx, address, count)
            logger.info(f"   Raw Values: {values}")

            # Try to decode values if they look like temperatures
            decoded_info = []
            for i, val in enumerate(values):
                addr = address + i
                if val == 0:
                    decoded_info.append(f"addr {addr}: 0 (empty)")
                elif 100 <= val <= 500:  # Likely temperature * 10
                    temp = val / 10.0
                    decoded_info.append(f"addr {addr}: {temp:.1f}°C")
                elif 1000 <= val <= 5000:  # Might be temperature * 100 or power
                    temp = val / 100.0
                    power = val
                    decoded_info.append(f"addr {addr}: {temp:.1f}°C OR {power}W")
                else:
                    decoded_info.append(f"addr {addr}: {val} (raw)")

            logger.info(f"   Decoded: {decoded_info}")

            # If reading 2 registers, also show as uint32
            if count == 2 and len(values) == 2:
                uint32_value = (values[0] << 16) | values[1]
                logger.info(f"   As uint32: {uint32_value}")
                if 1000 <= uint32_value <= 10000:  # Likely temperature * 100
                    temp = uint32_value / 100.0
                    logger.info(f"   As temperature: {temp:.2f}°C")

        except Exception as e:
            logger.error(f"   ERROR getting values: {e}")
            values = [0] * count

        logger.info("=" * 60)
        return values

    def setValues(self, fx, address, values):
        """Override to log all incoming write requests"""
        fx_names = {
            5: "Write Single Coil",
            6: "Write Single Register",
            15: "Write Multiple Coils",
            16: "Write Multiple Registers"
        }

        if fx > 5:
            fx_name = fx_names.get(fx, f"Unknown Function {fx}")
            logger.info("=" * 60)
            logger.info(f"📝 INCOMING WRITE REQUEST")
            logger.info(f"   Function Code: {fx} ({fx_name})")
            logger.info(f"   Start Address: {address}")
            logger.info(f"   Values: {values}")
            logger.info("=" * 60)

        return super().setValues(fx, address, values)


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
        """Create the Modbus data store with debugging"""
        # Create data blocks with wide address coverage
        # IMPORTANT: Use address 1 to handle Modbus 30001+ addressing correctly

        # Input registers (function 4) - start at address 1 for proper Modbus addressing
        input_registers = ModbusSequentialDataBlock(1, [0] * 65535)

        # Holding registers (function 3) - start at address 1
        holding_registers = ModbusSequentialDataBlock(1, [0] * 65535)

        # Coils and discrete inputs
        coils = ModbusSequentialDataBlock(1, [False] * 65535)
        discrete_inputs = ModbusSequentialDataBlock(1, [False] * 65535)

        # Create debugging device context
        device_context = DebuggingModbusDeviceContext(
            di=discrete_inputs,  # Discrete inputs
            co=coils,  # Coils
            hr=holding_registers,  # Holding registers (setpoints)
            ir=input_registers  # Input registers (sensor data)
        )

        # Create server context (single device, address 0)
        self.context = ModbusServerContext(devices=device_context, single=True)

        logger.info("📋 Datastore initialized with proper Modbus addressing (1-65535)")
        logger.info("   Input registers 30001-39999 will map correctly")
        logger.info("   Ready to capture ANY incoming requests...")

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
            self.CHILLER_BASE + 0: int(data.get('reservoir_temp', 0) * 10),  # 30001
        }

        # Update input registers (read-only sensor data)
        for address, value in registers.items():
            slave_context.setValues(4, address, [value])


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
        ]

        # Update input registers
        for i, value in enumerate(registers):
            slave_context.setValues(4, base_addr + i, [value])

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
                "setpoint": self.CHILLER_BASE + 10001,
            },
            "fermenters": {}
        }

        for fermenter_id, fermenter_index in self.fermenters.items():
            base = self.FERMENTER_BASE + (fermenter_index * self.REGISTERS_PER_FERMENTER)
            reg_map["fermenters"][fermenter_id] = {
                "base_address": base,
                "current_temp": base + 0,
                "setpoint": base + 10001,
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
    """Run minimal simulation to keep data fresh"""
    import math
    import time

    # Give server a moment to start
    await asyncio.sleep(1)

    # Add some fermenters
    server.add_fermenter("FV001")

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

    # Print register map
    print("\n" + "=" * 60)
    print("🔍 MODBUS DEBUGGING SERVER READY")
    print("=" * 60)
    print("\n🌐 Connect your gateway to this server")
    print("📝 All incoming requests will be logged below:")
    print("=" * 60)

    # Minimal simulation loop

    start_time = time.time()
    while True:
        elapsed = time.time() - start_time

        # Update chiller data occasionally
        chiller_data = {
            'reservoir_temp': 2.0 + 0.5 * math.sin(elapsed / 60),
        }
        server.update_chiller_data(chiller_data)

        # Update fermenter data
        for i, fv_id in enumerate(["FV001"]):
            fermenter_data = {
                'current_temp': 18.0 + i + 1.0 * math.sin(elapsed / 200 + i),
            }
            server.update_fermenter_data(fv_id, fermenter_data)

        await asyncio.sleep(30)  # Update every 30 seconds


async def main():
    """Debug-focused Modbus server"""
    # Create server
    server = BreweryModbusServer()

    # Show network info for debugging
    print("\n🌐 Network Information:")
    net_info = server.get_network_info()
    for key, value in net_info.items():
        print(f"   {key}: {value}")

    try:
        # Run server and simulation concurrently
        await asyncio.gather(
            server.start_server(),
            simulation_task(server)
        )
    except KeyboardInterrupt:
        print("\n\n🛑 Shutting down debug server...")
    finally:
        await server.stop_server()


if __name__ == "__main__":
    asyncio.run(main())