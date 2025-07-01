"""This module contains tests for the base IO classes."""
import datetime as dt
from unittest.mock import Mock

import pytest

from interloper.io.base import IO, IOContext, IOHandler, TypedIO
from interloper.partitioning.partition import TimePartition
from interloper.partitioning.window import TimePartitionWindow
from interloper.reconciler import Reconciler


class TestIOContext:
    """Test the IOContext class."""

    def test_io_context_creation_with_asset_only(self):
        """Test IOContext creation with only asset parameter."""
        asset = Mock()
        context = IOContext(asset=asset)

        assert context.asset == asset
        assert context.partition is None

    def test_io_context_creation_with_partition(self):
        """Test IOContext creation with partition."""
        asset = Mock()
        partition = TimePartition(dt.date(2024, 1, 1))
        context = IOContext(asset=asset, partition=partition)

        assert context.asset == asset
        assert context.partition == partition

    def test_io_context_creation_with_partition_window(self):
        """Test IOContext creation with partition window."""
        asset = Mock()
        partition_window = TimePartitionWindow(dt.date(2024, 1, 1), dt.date(2024, 1, 3))
        context = IOContext(asset=asset, partition=partition_window)

        assert context.asset == asset
        assert context.partition == partition_window

    def test_io_context_immutability(self):
        """Test that IOContext is immutable (frozen dataclass)."""
        asset = Mock()
        context = IOContext(asset=asset)

        with pytest.raises(Exception):  # Frozen dataclass raises exception on assignment
            context.asset = Mock()


class TestIO:
    """Test the IO class."""

    def test_io_abstract_methods(self):
        """Test that IO is an abstract base class with required methods."""
        # Should not be able to instantiate IO directly
        with pytest.raises(TypeError):
            IO()

    def test_io_concrete_implementation(self):
        """Test concrete implementation of IO."""

        class ConcreteIO(IO):
            def write(self, context: IOContext, data: any) -> None:
                pass

            def read(self, context: IOContext) -> any:
                return "test_data"

        io = ConcreteIO()
        assert isinstance(io, IO)


class TestIOHandler:
    """Test the IOHandler class."""

    def test_io_handler_abstract_methods(self):
        """Test that IOHandler is an abstract base class with required methods."""
        # Should not be able to instantiate IOHandler directly
        with pytest.raises(TypeError):
            IOHandler(type=str)

    def test_io_handler_concrete_implementation(self):
        """Test concrete implementation of IOHandler."""

        class StringIOHandler(IOHandler[str]):
            def write(self, context: IOContext, data: str) -> None:
                pass

            def read(self, context: IOContext) -> str:
                return "test_string"

        handler = StringIOHandler(type=str)
        assert handler.type is str
        assert handler.reconciler is None

    def test_io_handler_with_reconciler(self):
        """Test IOHandler with reconciler."""
        reconciler = Mock(spec=Reconciler)

        class StringIOHandler(IOHandler[str]):
            def write(self, context: IOContext, data: str) -> None:
                pass

            def read(self, context: IOContext) -> str:
                return "test_string"

        handler = StringIOHandler(type=str, reconciler=reconciler)
        assert handler.type is str
        assert handler.reconciler == reconciler

    def test_verify_type_correct_type(self):
        """Test verify_type with correct data type."""

        class StringIOHandler(IOHandler[str]):
            def write(self, context: IOContext, data: str) -> None:
                pass

            def read(self, context: IOContext) -> str:
                return "test_string"

        handler = StringIOHandler(type=str)
        # Should not raise an exception
        handler.verify_type("test_string")

    def test_verify_type_incorrect_type(self):
        """Test verify_type with incorrect data type."""

        class StringIOHandler(IOHandler[str]):
            def write(self, context: IOContext, data: str) -> None:
                pass

            def read(self, context: IOContext) -> str:
                return "test_string"

        handler = StringIOHandler(type=str)

        with pytest.raises(ValueError, match="Data type int is not supported"):
            handler.verify_type(42)

    def test_verify_type_with_generic_types(self):
        """Test verify_type with generic types like list[str]."""

        class ListStringIOHandler(IOHandler[list[str]]):
            def write(self, context: IOContext, data: list[str]) -> None:
                pass

            def read(self, context: IOContext) -> list[str]:
                return ["test"]

        handler = ListStringIOHandler(type=list[str])

        # Should not raise an exception
        handler.verify_type(["test", "string"])

        with pytest.raises(ValueError, match="Data type list is not supported"):
            handler.verify_type([1, 2, 3])


class TestTypedIO:
    """Test the TypedIO class."""

    def test_typed_io_abstract_methods(self):
        """Test that TypedIO is an abstract base class with required methods."""
        # Should not be able to instantiate TypedIO directly
        with pytest.raises(TypeError):
            TypedIO([])

    def test_typed_io_concrete_implementation(self):
        """Test concrete implementation of TypedIO."""

        class ConcreteTypedIO(TypedIO):
            def write(self, context: IOContext, data: any) -> None:
                handler = self.get_handler(type(data))
                handler.write(context, data)

            def read(self, context: IOContext) -> any:
                # For testing, we'll use a mock asset with data_type
                mock_asset = Mock()
                mock_asset.data_type = str
                context.asset = mock_asset
                handler = self.get_handler(str)
                return handler.read(context)

        class StringHandler(IOHandler[str]):
            def write(self, context: IOContext, data: str) -> None:
                pass

            def read(self, context: IOContext) -> str:
                return "test"

        handlers = [StringHandler(type=str)]
        typed_io = ConcreteTypedIO(handlers)
        assert isinstance(typed_io, TypedIO)

    def test_typed_io_initialization(self):
        """Test TypedIO initialization with handlers."""

        class ConcreteTypedIO(TypedIO):
            def write(self, context: IOContext, data: any) -> None:
                pass

            def read(self, context: IOContext) -> any:
                return "test"

        class StringHandler(IOHandler[str]):
            def write(self, context: IOContext, data: str) -> None:
                pass

            def read(self, context: IOContext) -> str:
                return "test"

        class IntHandler(IOHandler[int]):
            def write(self, context: IOContext, data: int) -> None:
                pass

            def read(self, context: IOContext) -> int:
                return 42

        handlers = [StringHandler(type=str), IntHandler(type=int)]
        typed_io = ConcreteTypedIO(handlers)

        assert len(typed_io._handlers) == 2
        assert str in typed_io._handlers
        assert int in typed_io._handlers

    def test_supported_types_property(self):
        """Test supported_types property."""

        class ConcreteTypedIO(TypedIO):
            def write(self, context: IOContext, data: any) -> None:
                pass

            def read(self, context: IOContext) -> any:
                return "test"

        class StringHandler(IOHandler[str]):
            def write(self, context: IOContext, data: str) -> None:
                pass

            def read(self, context: IOContext) -> str:
                return "test"

        handlers = [StringHandler(type=str)]
        typed_io = ConcreteTypedIO(handlers)

        assert typed_io.supported_types == {str}

    def test_get_handler_exact_match(self):
        """Test get_handler with exact type match."""

        class ConcreteTypedIO(TypedIO):
            def write(self, context: IOContext, data: any) -> None:
                pass

            def read(self, context: IOContext) -> any:
                return "test"

        class StringHandler(IOHandler[str]):
            def write(self, context: IOContext, data: str) -> None:
                pass

            def read(self, context: IOContext) -> str:
                return "test"

        handlers = [StringHandler(type=str)]
        typed_io = ConcreteTypedIO(handlers)

        handler = typed_io.get_handler(str)
        assert isinstance(handler, StringHandler)

    def test_get_handler_with_generic_types(self):
        """Test get_handler with generic types."""

        class ConcreteTypedIO(TypedIO):
            def write(self, context: IOContext, data: any) -> None:
                pass

            def read(self, context: IOContext) -> any:
                return "test"

        class ListStringHandler(IOHandler[list[str]]):
            def write(self, context: IOContext, data: list[str]) -> None:
                pass

            def read(self, context: IOContext) -> list[str]:
                return ["test"]

        handlers = [ListStringHandler(type=list[str])]
        typed_io = ConcreteTypedIO(handlers)

        handler = typed_io.get_handler(list[str])
        assert isinstance(handler, ListStringHandler)

    def test_get_handler_none_type(self):
        """Test get_handler with None type raises error."""

        class ConcreteTypedIO(TypedIO):
            def write(self, context: IOContext, data: any) -> None:
                pass

            def read(self, context: IOContext) -> any:
                return "test"

        class StringHandler(IOHandler[str]):
            def write(self, context: IOContext, data: str) -> None:
                pass

            def read(self, context: IOContext) -> str:
                return "test"

        handlers = [StringHandler(type=str)]
        typed_io = ConcreteTypedIO(handlers)

        with pytest.raises(RuntimeError, match="requires the asset to have a data type"):
            typed_io.get_handler(None)

    def test_get_handler_unsupported_type(self):
        """Test get_handler with unsupported type raises error."""

        class ConcreteTypedIO(TypedIO):
            def write(self, context: IOContext, data: any) -> None:
                pass

            def read(self, context: IOContext) -> any:
                return "test"

        class StringHandler(IOHandler[str]):
            def write(self, context: IOContext, data: str) -> None:
                pass

            def read(self, context: IOContext) -> str:
                return "test"

        handlers = [StringHandler(type=str)]
        typed_io = ConcreteTypedIO(handlers)

        with pytest.raises(RuntimeError, match="does not support data type int"):
            typed_io.get_handler(int)

    def test_write_method(self):
        """Test TypedIO write method."""

        class ConcreteTypedIO(TypedIO):
            def write(self, context: IOContext, data: any) -> None:
                handler = self.get_handler(type(data))
                handler.write(context, data)

            def read(self, context: IOContext) -> any:
                return "test"

        class StringHandler(IOHandler[str]):
            def __init__(self):
                super().__init__(type=str)
                self.written_data = None

            def write(self, context: IOContext, data: str) -> None:
                self.written_data = data

            def read(self, context: IOContext) -> str:
                return "test"

        handler = StringHandler()
        typed_io = ConcreteTypedIO([handler])
        context = IOContext(asset=Mock())

        typed_io.write(context, "test_data")
        assert handler.written_data == "test_data"

    def test_read_method(self):
        """Test TypedIO read method."""

        class ConcreteTypedIO(TypedIO):
            def write(self, context: IOContext, data: any) -> None:
                pass

            def read(self, context: IOContext) -> any:
                # Create a new context with the proper asset
                mock_asset = Mock()
                mock_asset.data_type = str
                new_context = IOContext(asset=mock_asset)
                handler = self.get_handler(str)
                return handler.read(new_context)

        class StringHandler(IOHandler[str]):
            def write(self, context: IOContext, data: str) -> None:
                pass

            def read(self, context: IOContext) -> str:
                return "test_data"

        handler = StringHandler(type=str)
        typed_io = ConcreteTypedIO([handler])
        context = IOContext(asset=Mock())

        result = typed_io.read(context)
        assert result == "test_data"

    def test_write_with_type_verification(self):
        """Test that write method verifies data type."""

        class ConcreteTypedIO(TypedIO):
            def write(self, context: IOContext, data: any) -> None:
                handler = self.get_handler(type(data))
                handler.verify_type(data)
                handler.write(context, data)

            def read(self, context: IOContext) -> any:
                return "test"

        class StringHandler(IOHandler[str]):
            def write(self, context: IOContext, data: str) -> None:
                pass

            def read(self, context: IOContext) -> str:
                return "test"

        handler = StringHandler(type=str)
        typed_io = ConcreteTypedIO([handler])
        context = IOContext(asset=Mock())

        # Should not raise an exception
        typed_io.write(context, "test_string")

        # Should raise an exception for wrong type
        with pytest.raises(RuntimeError, match="does not support data type int"):
            typed_io.write(context, 42)

    def test_read_with_type_verification(self):
        """Test that read method verifies return type."""

        class ConcreteTypedIO(TypedIO):
            def write(self, context: IOContext, data: any) -> None:
                pass

            def read(self, context: IOContext) -> any:
                # Create a new context with the proper asset
                mock_asset = Mock()
                mock_asset.data_type = str
                new_context = IOContext(asset=mock_asset)
                handler = self.get_handler(str)
                data = handler.read(new_context)
                handler.verify_type(data)
                return data

        class StringHandler(IOHandler[str]):
            def write(self, context: IOContext, data: str) -> None:
                pass

            def read(self, context: IOContext) -> str:
                return 42  # Wrong type, should be str

        handler = StringHandler(type=str)
        typed_io = ConcreteTypedIO([handler])
        context = IOContext(asset=Mock())

        # Should raise an exception for wrong return type
        with pytest.raises(ValueError, match="Data type int is not supported"):
            typed_io.read(context)

    def test_multiple_handlers_same_base_type(self):
        """Test TypedIO with multiple handlers for different specializations."""

        class ConcreteTypedIO(TypedIO):
            def write(self, context: IOContext, data: any) -> None:
                pass

            def read(self, context: IOContext) -> any:
                return "test"

        class ListHandler(IOHandler[list]):
            def write(self, context: IOContext, data: list) -> None:
                pass

            def read(self, context: IOContext) -> list:
                return []

        class ListStringHandler(IOHandler[list[str]]):
            def write(self, context: IOContext, data: list[str]) -> None:
                pass

            def read(self, context: IOContext) -> list[str]:
                return ["test"]

        handlers = [ListHandler(type=list), ListStringHandler(type=list[str])]
        typed_io = ConcreteTypedIO(handlers)

        # Should get the general handler for list[str] (current implementation)
        handler = typed_io.get_handler(list[str])
        assert isinstance(handler, ListHandler)

        # Should get the general handler for list
        handler = typed_io.get_handler(list)
        assert isinstance(handler, ListHandler)

    def test_empty_handlers_list(self):
        """Test TypedIO with empty handlers list."""

        class ConcreteTypedIO(TypedIO):
            def write(self, context: IOContext, data: any) -> None:
                pass

            def read(self, context: IOContext) -> any:
                return "test"

        typed_io = ConcreteTypedIO([])

        assert typed_io.supported_types == set()

        with pytest.raises(RuntimeError, match="does not support data type str"):
            typed_io.get_handler(str)

    def test_duplicate_handler_types(self):
        """Test TypedIO with duplicate handler types (should use last one)."""

        class ConcreteTypedIO(TypedIO):
            def write(self, context: IOContext, data: any) -> None:
                pass

            def read(self, context: IOContext) -> any:
                # Create a new context with the proper asset
                mock_asset = Mock()
                mock_asset.data_type = str
                new_context = IOContext(asset=mock_asset)
                handler = self.get_handler(str)
                return handler.read(new_context)

        class StringHandler1(IOHandler[str]):
            def write(self, context: IOContext, data: str) -> None:
                pass

            def read(self, context: IOContext) -> str:
                return "handler1"

        class StringHandler2(IOHandler[str]):
            def write(self, context: IOContext, data: str) -> None:
                pass

            def read(self, context: IOContext) -> str:
                return "handler2"

        handlers = [StringHandler1(type=str), StringHandler2(type=str)]
        typed_io = ConcreteTypedIO(handlers)

        # Should use the last handler
        handler = typed_io.get_handler(str)
        assert isinstance(handler, StringHandler2)

        context = IOContext(asset=Mock())
        result = typed_io.read(context)
        assert result == "handler2"
