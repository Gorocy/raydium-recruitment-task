import json
from raydium_parser.raydium_parser import parse_block, RaydiumSwap
from raydium_parser.rpc_utils import get_block


def test_raydium_parser():
    block = get_block(316719543)

    swaps = parse_block(block, 316719543)

    # Convert the iterator to a list to print all swaps
    swaps_list = list(swaps)

    assert len(swaps_list) == 809, "No swaps were parsed"

    # Filter swaps to only include successful ones
    successful_swaps = [swap for swap in swaps_list if swap.was_successful]

    # Calculate expected successful count and margin of error
    expected_successful_count = round(809 * 0.06)
    margin_of_error = round(809 * 0.006)

    # Calculate lower and upper bounds
    lower_bound = expected_successful_count - margin_of_error
    upper_bound = expected_successful_count + margin_of_error

    # Check if the number of successful swaps is within the margin of error
    assert lower_bound <= len(successful_swaps) <= upper_bound, (
        f"Expected successful swaps to be between {lower_bound} and {upper_bound}, "
        f"but got {len(successful_swaps)}"
    )
