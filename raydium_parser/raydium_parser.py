from collections.abc import Iterator
from dataclasses import dataclass
from typing import Literal, cast, Union, Optional

from solders.instruction import CompiledInstruction
from solders.message import Message
from solders.signature import Signature
from solders.pubkey import Pubkey
from solders.transaction import Transaction
from solders.transaction_status import (
    EncodedTransactionWithStatusMeta,
    UiConfirmedBlock,
    UiParsedInstruction,
    UiCompiledInstruction,
    UiTransactionStatusMeta,
)
import base58

# Extended list of Raydium programs
RAYDIUM_PROGRAMS = {
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",  # Legacy AMM v4
    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",  # Standard AMM
    "5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h",  # Stable Swap AMM
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",  # Concentrated Liquidity
    "routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS",   # AMM Routing
    "27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv",  # Legacy AMM v3
    "EhhTKczWMGQt46ynNeRX1WfeagwwJd7ufHvCDjRxjo5Q",  # Legacy AMM v2
    "RVKd61ztZW9GUwhRbbLoYVRE5Xf1B2tVscKqwZqXgEr",   # Raydium Vault
}

# Extended instruction discriminators for different AMM types
SWAP_INSTRUCTION_DISCRIMINATOR = {
    # Standard AMM
    9,   # swap
    11,  # route swap

    # Legacy AMM
    1,   # swap
    2,   # route swap

    # Concentrated Liquidity (CL)
    4,   # swap
    5,   # swap v2
    6,   # swap by input amount
    7,   # swap by output amount
}


@dataclass
class RaydiumSwap:
    slot: int
    index_in_slot: int
    index_in_tx: int

    signature: str

    was_successful: bool

    mint_in: Pubkey
    mint_out: Pubkey
    amount_in: int
    amount_out: int

    limit_amount: int
    limit_side: Literal["mint_in", "mint_out"]

    post_pool_balance_mint_in: int
    post_pool_balance_mint_out: int


def parse_block(block: UiConfirmedBlock, slot: int) -> Iterator[RaydiumSwap]:
    """
    Processes a block of transactions and returns an iterator of RaydiumSwap objects.
    """
    if not block.transactions:
        print("No transactions found in block")
        return

    swaps_found = 0
    instruction_count = 0
    failed_swaps = {prog: 0 for prog in RAYDIUM_PROGRAMS}

    for tx_index, tx in enumerate(block.transactions):
        if not tx or not hasattr(tx, "transaction") or not hasattr(tx, "meta"):
            continue

        tx = cast(EncodedTransactionWithStatusMeta, tx)
        meta = cast(UiTransactionStatusMeta, tx.meta)
        transaction = cast(Transaction, tx.transaction)
        message = cast(Message, transaction.message)
        was_successful = meta.err is None

        # Get complete list of accounts including loaded addresses
        all_account_keys = list(message.account_keys)
        if hasattr(meta, "loaded_addresses") and meta.loaded_addresses:
            if hasattr(meta.loaded_addresses, "writable"):
                all_account_keys.extend(meta.loaded_addresses.writable)
            if hasattr(meta.loaded_addresses, "readonly"):
                all_account_keys.extend(meta.loaded_addresses.readonly)

        # Process main instructions
        for ix_index, ix in enumerate(message.instructions):
            instruction_count += 1
            try:
                ix = cast(Union[UiParsedInstruction, UiCompiledInstruction, CompiledInstruction], ix)
                
                if isinstance(ix, UiParsedInstruction):
                    program_id = extract_program_id_from_parsed(ix)
                else:
                    if ix.program_id_index >= len(all_account_keys):
                        continue
                    program_id = str(all_account_keys[ix.program_id_index])
                
                if program_id in RAYDIUM_PROGRAMS:
                    swap = try_parse_raydium_swap(
                        ix, message, meta, transaction, slot, tx_index, ix_index, was_successful, all_account_keys
                    )
                    if swap:
                        swaps_found += 1
                        yield swap
                    else:
                        failed_swaps[program_id] = failed_swaps.get(program_id, 0) + 1
            except Exception as e:
                print(f"Error processing instruction {ix_index}: {e}")
                continue

        # Process inner instructions
        inner_instructions = getattr(meta, "inner_instructions", None) or []
        for inner_set in inner_instructions:
            for inner_ix_index, inner_ix in enumerate(inner_set.instructions):
                instruction_count += 1
                try:
                    inner_ix = cast(Union[UiParsedInstruction, UiCompiledInstruction, CompiledInstruction], inner_ix)
                    
                    if isinstance(inner_ix, UiParsedInstruction):
                        inner_program_id = extract_program_id_from_parsed(inner_ix)
                    else:
                        if inner_ix.program_id_index >= len(all_account_keys):
                            continue
                        inner_program_id = str(all_account_keys[inner_ix.program_id_index])
                    
                    if inner_program_id in RAYDIUM_PROGRAMS:
                        try:
                            inner_swap = try_parse_raydium_swap(
                                inner_ix, message, meta, transaction, slot, tx_index, 
                                inner_ix_index, was_successful, all_account_keys
                            )
                            if inner_swap:
                                swaps_found += 1
                                yield inner_swap
                            else:
                                failed_swaps[inner_program_id] = failed_swaps.get(inner_program_id, 0) + 1
                        except ValueError:
                            continue
                except Exception:
                    continue

    print(f"\nSwap parsing statistics for block {slot}:")
    print(f"Total swaps found: {swaps_found}")
    print(f"Total instructions processed: {instruction_count}")
    print("\nFailed swaps by program:")
    for prog, count in failed_swaps.items():
        if count > 0:
            print(f"{prog}: {count} failed attempts")


def try_parse_raydium_swap(
    ix: Union[UiParsedInstruction, UiCompiledInstruction, CompiledInstruction],
    message: Message,
    meta: UiTransactionStatusMeta,
    transaction: Transaction,
    slot: int,
    tx_index: int,
    ix_index: int,
    was_successful: bool,
    all_account_keys: list[Pubkey],
) -> Optional[RaydiumSwap]:
    """
    Attempts to parse an instruction into a RaydiumSwap object.
    """
    if isinstance(ix, UiParsedInstruction):
        return parse_raydium_swap_from_ui_parsed_instruction(
            ix, message, meta, transaction, slot, tx_index, ix_index, was_successful, all_account_keys
        )
    elif isinstance(ix, CompiledInstruction):
        return parse_raydium_swap_from_compiled_instruction(
            ix, message, meta, transaction, slot, tx_index, ix_index, was_successful, all_account_keys
        )
    else:
        return parse_raydium_swap_from_ui_compiled_instruction(
            ix, message, meta, transaction, slot, tx_index, ix_index, was_successful, all_account_keys
        )


def _parse_swap_instruction(ix: UiCompiledInstruction) -> tuple[int, int]:
    """
    Decodes swap instruction data and returns a tuple (amount_in, min_amount_out).
    """
    data = base58.b58decode(ix.data)
    
    # Format for Concentrated Liquidity (CL) swaps
    if len(data) >= 33 and data[0] in {4, 5, 6, 7}:
        if data[0] in {4, 5}:  # Basic CL swap
            amount_in = int.from_bytes(data[1:9], "little")
            min_amount_out = int.from_bytes(data[9:17], "little")
        elif data[0] == 6:  # Swap by input
            amount_in = int.from_bytes(data[1:9], "little")
            min_amount_out = int.from_bytes(data[17:25], "little")
        else:  # Swap by output
            amount_in = int.from_bytes(data[9:17], "little")
            min_amount_out = int.from_bytes(data[1:9], "little")
        return amount_in, min_amount_out
        
    # Format for Standard/Legacy AMM swaps
    if len(data) >= 17 and data[0] in SWAP_INSTRUCTION_DISCRIMINATOR:
        amount_in = int.from_bytes(data[1:9], "little")
        min_amount_out = int.from_bytes(data[9:17], "little")
        return amount_in, min_amount_out
        
    raise ValueError(f"Unsupported swap instruction format: {data[0]}")


def determine_pool_indices_generic(ix: Union[UiCompiledInstruction, CompiledInstruction], message: Message) -> tuple[int, int]:
    """
    Returns pool account indices (pool_in, pool_out) from the 'accounts' attribute of the instruction.
    """
    accounts = getattr(ix, "accounts", None)
    if not accounts:
        raise ValueError("No accounts in instruction")
        
    # Format for Concentrated Liquidity (CL)
    if len(accounts) >= 13:  # CL format
        pool_in_index = accounts[7]   # Token vault A
        pool_out_index = accounts[8]  # Token vault B
    # Format for Standard/Legacy AMM
    elif len(accounts) >= 7:
        pool_in_index = accounts[5]
        pool_out_index = accounts[6]
    else:
        raise ValueError("Not enough accounts in instruction")
    
    return pool_in_index, pool_out_index


def _is_valid_swap_instruction(ix: Union[UiCompiledInstruction, CompiledInstruction]) -> bool:
    """
    Validates if the instruction is a valid swap.
    """
    try:
        data = base58.b58decode(getattr(ix, 'data', ''))
        if len(data) < 1:
            return False
            
        # Validate for CL swaps
        if len(data) >= 33 and data[0] in {4, 5, 6, 7}:
            accounts = getattr(ix, "accounts", None)
            if accounts is None or len(accounts) < 13:
                print(f"Invalid CL swap instruction: insufficient accounts, data length: {len(data)}")
                return False
            return True
            
        # Validate for Standard/Legacy swaps
        if len(data) >= 17 and data[0] in SWAP_INSTRUCTION_DISCRIMINATOR:
            accounts = getattr(ix, "accounts", None)
            if accounts is None or len(accounts) < 7:
                print(f"Invalid standard swap instruction: insufficient accounts, data length: {len(data)}")
                return False
            return True
            
        print(f"Invalid swap instruction: unknown format, discriminator: {data[0]}")
        return False
    except Exception as e:
        print(f"Error validating swap instruction: {e}")
        return False


def parse_raydium_swap_from_ui_compiled_instruction(
    ix: UiCompiledInstruction,
    message: Message,
    meta: UiTransactionStatusMeta,
    transaction: Transaction,
    slot: int,
    tx_index: int,
    ix_index: int,
    was_successful: bool,
    all_account_keys: list[Pubkey],
) -> Optional[RaydiumSwap]:
    """
    Processes a UiCompiledInstruction as a potential swap.
    """
    if not _is_valid_swap_instruction(ix):
        print("Invalid swap instruction (UiCompiledInstruction)")
        return None

    try:
        amount_in, min_amount_out = _parse_swap_instruction(ix)
        pool_in_index, pool_out_index = determine_pool_indices_generic(ix, message)

        pool_in_balance = 0
        pool_out_balance = 0
        if meta.post_token_balances:
            for balance in meta.post_token_balances:
                if balance.account_index == pool_in_index:
                    pool_in_balance = balance.ui_token_amount.amount
                elif balance.account_index == pool_out_index:
                    pool_out_balance = balance.ui_token_amount.amount

        account_keys = all_account_keys
        mint_in = account_keys[pool_in_index]
        mint_out = account_keys[pool_out_index]
        signature = cast(Signature, transaction.signatures[0])
        return RaydiumSwap(
            slot=slot,
            index_in_slot=tx_index,
            index_in_tx=ix_index,
            signature=str(signature),
            was_successful=was_successful,
            mint_in=mint_in,
            mint_out=mint_out,
            amount_in=amount_in,
            amount_out=min_amount_out,
            limit_amount=min_amount_out,
            limit_side="mint_out",
            post_pool_balance_mint_in=int(pool_in_balance),
            post_pool_balance_mint_out=int(pool_out_balance),
        )
    except Exception as e:
        print(f"Error processing UiCompiledInstruction: {e}")
        return None


def parse_raydium_swap_from_compiled_instruction(
    ix: CompiledInstruction,
    message: Message,
    meta: UiTransactionStatusMeta,
    transaction: Transaction,
    slot: int,
    tx_index: int,
    ix_index: int,
    was_successful: bool,
    all_account_keys: list[Pubkey],
) -> Optional[RaydiumSwap]:
    """
    Processes a CompiledInstruction as a potential swap.
    """
    try:
        if not hasattr(ix, "data"):
            print("CompiledInstruction missing data attribute")
            return None

        if not _is_valid_swap_instruction(ix):
            print("Invalid swap instruction (CompiledInstruction)")
            return None

        amount_in, min_amount_out = _parse_swap_instruction(cast(UiCompiledInstruction, ix))
        pool_in_index, pool_out_index = determine_pool_indices_generic(ix, message)

        pool_in_balance = 0
        pool_out_balance = 0
        if meta.post_token_balances:
            for balance in meta.post_token_balances:
                if balance.account_index == pool_in_index:
                    pool_in_balance = balance.ui_token_amount.amount
                elif balance.account_index == pool_out_index:
                    pool_out_balance = balance.ui_token_amount.amount

        account_keys = all_account_keys
        mint_in = account_keys[pool_in_index]
        mint_out = account_keys[pool_out_index]
        signature = cast(Signature, transaction.signatures[0])
        return RaydiumSwap(
            slot=slot,
            index_in_slot=tx_index,
            index_in_tx=ix_index,
            signature=str(signature),
            was_successful=was_successful,
            mint_in=mint_in,
            mint_out=mint_out,
            amount_in=amount_in,
            amount_out=min_amount_out,
            limit_amount=min_amount_out,
            limit_side="mint_out",
            post_pool_balance_mint_in=int(pool_in_balance),
            post_pool_balance_mint_out=int(pool_out_balance),
        )
    except Exception as e:
        print(f"Error processing CompiledInstruction: {e}")
        return None


def parse_raydium_swap_from_ui_parsed_instruction(
    ix: UiParsedInstruction,
    message: Message,
    meta: UiTransactionStatusMeta,
    transaction: Transaction,
    slot: int,
    tx_index: int,
    ix_index: int,
    was_successful: bool,
    all_account_keys: list[Pubkey],
) -> Optional[RaydiumSwap]:
    """
    Processes a UiParsedInstruction as a potential swap.
    Assumes that parsed contains a dict with a "type" key equal to "swap".
    """
    try:
        parsed_data = getattr(ix, "parsed", None)
        if not isinstance(parsed_data, dict) or parsed_data.get("type") != "swap":
            print("UiParsedInstruction is not a swap type")
            return None

        amount_in = int(parsed_data.get("amountIn", 0))
        min_amount_out = int(parsed_data.get("minAmountOut", 0))

        # Determine pool indices based on additional data – can be customized
        pool_in_index, pool_out_index = determine_pool_indices_for_parsed(ix, message)

        pool_in_balance = 0
        pool_out_balance = 0
        if meta.post_token_balances:
            for balance in meta.post_token_balances:
                if balance.account_index == pool_in_index:
                    pool_in_balance = balance.ui_token_amount.amount
                elif balance.account_index == pool_out_index:
                    pool_out_balance = balance.ui_token_amount.amount

        account_keys = all_account_keys
        if pool_in_index >= len(account_keys) or pool_out_index >= len(account_keys):
            raise ValueError("Pool indices for parsed instruction are out of range")
        mint_in = account_keys[pool_in_index]
        mint_out = account_keys[pool_out_index]
        signature = cast(Signature, transaction.signatures[0])
        return RaydiumSwap(
            slot=slot,
            index_in_slot=tx_index,
            index_in_tx=ix_index,
            signature=str(signature),
            was_successful=was_successful,
            mint_in=mint_in,
            mint_out=mint_out,
            amount_in=amount_in,
            amount_out=min_amount_out,
            limit_amount=min_amount_out,
            limit_side="mint_out",
            post_pool_balance_mint_in=int(pool_in_balance),
            post_pool_balance_mint_out=int(pool_out_balance),
        )
    except Exception as e:
        print(f"Error processing UiParsedInstruction: {e}")
        return None


def determine_pool_indices_for_parsed(ix: UiParsedInstruction, message: Message) -> tuple[int, int]:
    """
    Determines pool indices based on parsed data.
    Customize the logic to match the actual data structure.
    """
    parsed_data = getattr(ix, "parsed", None)
    if isinstance(parsed_data, dict):
        pool_in_index = parsed_data.get("poolInIndex", -1)
        pool_out_index = parsed_data.get("poolOutIndex", -1)
        return pool_in_index, pool_out_index
    return -1, -1


def extract_program_id_from_parsed(ix: UiParsedInstruction) -> str:
    """
    Extracts program ID from UiParsedInstruction.
    """
    parsed_data = getattr(ix, "parsed", None)
    if isinstance(parsed_data, dict):
        return parsed_data.get("programId", "")
    return ""
