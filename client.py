#!/usr/bin/env python3
"""
Simple Modbus TCP Client for Brewery Server
Connects to the server and reads registers
"""
import asyncio
import logging
from pymodbus.client import AsyncModbusTcpClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    """Connect to Modbus server and read register 30002"""
    # Server configuration (matching main.py defaults)
    host = 'raspberrypi.local'  # or '127.0.0.1'
    port = 502
    
    # Create client
    client = AsyncModbusTcpClient(host, port=port)
    
    try:
        # Connect to server
        logger.info(f"Connecting to Modbus server at {host}:{port}")
        await client.connect()
        
        if not client.connected:
            logger.error("Failed to connect to server")
            return
            
        logger.info("Connected successfully!")
        
        # Read register 30002 (chiller supply temperature)
        # Function code 4 (read input registers)
        # Address 30002 (supply_temp in chiller block)
        result = await client.read_input_registers(30002, 1, slave=0)
        
        if result.isError():
            logger.error(f"Error reading register: {result}")
        else:
            # Temperature is stored scaled by 10
            raw_value = result.registers[0]
            temperature = raw_value / 10.0
            logger.info(f"Register 30002 value: {raw_value} (Temperature: {temperature}°C)")
            
        # Read a few more chiller registers for demonstration
        logger.info("\n=== Reading Chiller Registers ===")
        chiller_registers = [
            (30001, "reservoir_temp"),
            (30002, "supply_temp"),
            (30003, "return_temp"),
            (30004, "compressor_running"),
            (30005, "compressor_power"),
        ]
        
        for addr, name in chiller_registers:
            result = await client.read_input_registers(addr, 1, slave=0)
            if not result.isError():
                raw_value = result.registers[0]
                if "temp" in name:
                    value = raw_value / 10.0
                    logger.info(f"{name} ({addr}): {value}°C")
                elif name == "compressor_running":
                    status = "ON" if raw_value else "OFF"
                    logger.info(f"{name} ({addr}): {status}")
                else:
                    logger.info(f"{name} ({addr}): {raw_value}")
            else:
                logger.error(f"Error reading {name}: {result}")
                
    except Exception as e:
        logger.error(f"Connection error: {e}")
    finally:
        # Close connection
        await client.close()
        logger.info("Connection closed")


if __name__ == "__main__":
    asyncio.run(main())