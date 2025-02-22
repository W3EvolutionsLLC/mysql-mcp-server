from . import server_new
import asyncio

def main():
   """Main entry point for the package."""
   asyncio.run(server_new.main())

# Expose important items at package level
__all__ = ['main', 'server']