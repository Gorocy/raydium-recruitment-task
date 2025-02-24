from collections.abc import Iterator
from dataclasses import dataclass
from typing import Literal, cast, Optional, Tuple, TypeAlias

from solders.message import Message
from solders.signature import Signature
from solders.pubkey import Pubkey
from solders.transaction import Transaction
from solders.transaction_status import (
    EncodedTransactionWithStatusMeta,
    UiConfirmedBlock,
    UiCompiledInstruction,
    UiTransactionStatusMeta,
    UiTransactionTokenBalance
)
import base58
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Extended list of Raydium programs
RAYDIUM_PROGRAMS = {
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",  # Legacy AMM v4
}

# Instruction discriminators for Standard/Legacy AMM types
SWAP_IN_INSTRUCTION_DISCRIMINATOR = 9
SWAP_OUT_INSTRUCTION_DISCRIMINATOR = 11
SWAP_INSTRUCTION_DISCRIMINATOR = [SWAP_IN_INSTRUCTION_DISCRIMINATOR, SWAP_OUT_INSTRUCTION_DISCRIMINATOR]

PoolIndices: TypeAlias = Tuple[int, int]
BalanceDiff: TypeAlias = Tuple[int, int, bool]
PoolBalances: TypeAlias = Tuple[int, int]
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


def get_mint_in_out(pool_in_index: int, pool_out_index: int, meta: UiTransactionStatusMeta) -> tuple[Pubkey, Pubkey]:
    """
    Returns the mint_in and mint_out Pubkeys from the account keys list.
    """
    mint_in = None
    mint_out = None

    if meta.post_token_balances:
        for account in meta.post_token_balances:
            if account.account_index == pool_in_index:
                mint_in = account.mint
            elif account.account_index == pool_out_index:
                mint_out = account.mint
    
    if mint_in is None or mint_out is None:
        raise ValueError("Mint in or mint out not found")

    return mint_in, mint_out

def parse_block(block: UiConfirmedBlock, slot: int) -> Iterator[RaydiumSwap]:
    """
    Processes a block of transactions and returns an iterator of RaydiumSwap objects.
    """
    if not block.transactions:
        logger.info("No transactions found in block")
        return

    swaps_found = 0
    instruction_count = 0

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

        # Collect all instructions
        all_instructions = []
        all_instructions.extend(message.instructions)
        if meta.inner_instructions:
            for inner_ix in meta.inner_instructions:
                all_instructions.extend(inner_ix.instructions)
                
        # Process main instructions
        for ix_index, ix in enumerate(all_instructions):
            instruction_count += 1
            try:
                # Cast to UiCompiledInstruction since we only handle those
                if not isinstance(ix, UiCompiledInstruction):
                    continue
                
                if ix.program_id_index >= len(all_account_keys):
                    continue

                program_id = str(all_account_keys[ix.program_id_index])
                
                if program_id in RAYDIUM_PROGRAMS:
                    swap = parse_raydium_swap_from_ui_compiled_instruction(
                        ix, message, meta, transaction, slot, tx_index, ix_index, was_successful
                    )
                    if swap:
                        swaps_found += 1
                        yield swap
            except Exception as e:
                logger.error(f"Error processing instruction {ix_index}: {e}")
                continue

    logger.info(f"\nSwap parsing statistics for block {slot}:")
    logger.info(f"Total swaps found: {swaps_found}")
    logger.info(f"Total instructions processed: {instruction_count}")


def _parse_swap_instruction(ix: UiCompiledInstruction) -> tuple[int, Literal["mint_in", "mint_out"]]:
    """
    Decodes swap instruction data and returns a tuple (limit_amount, limit_side).
    """
    data = base58.b58decode(ix.data)
    discriminator = data[0]
    # Format for Standard/Legacy AMM swaps
    if discriminator == SWAP_IN_INSTRUCTION_DISCRIMINATOR:
        limit_amount = int.from_bytes(data[9:17], "little")
        return limit_amount, "mint_out"
    elif discriminator == SWAP_OUT_INSTRUCTION_DISCRIMINATOR:
        limit_amount = int.from_bytes(data[1:9], "little")
        return limit_amount, "mint_in"

    raise ValueError(f"Unsupported swap instruction format: {data[0]}")


def determine_pool_indices_generic(ix: UiCompiledInstruction, message: Message) -> PoolIndices:
    """
    Returns pool account indices (pool_in, pool_out) from the 'accounts' attribute of the instruction.
    """
    accounts = getattr(ix, "accounts", None)
    if not accounts:
        raise ValueError("No accounts in instruction")
    
    # Format for Standard/Legacy AMM
    if len(accounts) == 18:
        pool_in_index = accounts[5]
        pool_out_index = accounts[6]
    elif len(accounts) == 17:
        pool_in_index = accounts[4]
        pool_out_index = accounts[5]
    else:
        raise ValueError("Wrong number of accounts in instruction")
    
    return pool_in_index, pool_out_index


def _is_valid_swap_instruction(ix: UiCompiledInstruction) -> bool:
    """
    Validates if the instruction is a valid swap.
    """
    try:
        data = base58.b58decode(getattr(ix, 'data', ''))
        if len(data) < 1:
            return False
            
        # Validate for Standard/Legacy swaps
        if len(data) >= 17 and data[0] in SWAP_INSTRUCTION_DISCRIMINATOR:
            accounts = getattr(ix, "accounts", None)
            if accounts is None or len(accounts) < 7:
                logger.info(f"Invalid standard swap instruction: insufficient accounts, data length: {len(data)}")
                return False
            return True
            
        logger.info(f"Invalid swap instruction: unknown format, discriminator: {data[0]}")
        return False
    except Exception as e:
        logger.error(f"Error validating swap instruction: {e}")
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
) -> Optional[RaydiumSwap]:
    """
    Processes a UiCompiledInstruction as a potential swap.
    """
    if not _is_valid_swap_instruction(ix):
        logger.info("Invalid swap instruction (UiCompiledInstruction)")
        return None

    try:
        limit_amount, limit_side = _parse_swap_instruction(ix)

        pool_from_index, pool_to_index = determine_pool_indices_generic(ix, message)

        mint_in, mint_out = get_mint_in_out(pool_from_index, pool_to_index, meta)

        diff_from, diff_to, should_swap_direction = change_direction(meta, pool_from_index, pool_to_index)
        if should_swap_direction:

            diff_from, diff_to = diff_to, diff_from
            mint_in, mint_out = mint_out, mint_in
            pool_from_index, pool_to_index = pool_to_index, pool_from_index

        pool_in_balance, pool_out_balance = get_pool_balances(meta, pool_from_index, pool_to_index)
        
        signature = cast(Signature, transaction.signatures[0])
        return RaydiumSwap(
            slot=slot,
            index_in_slot=tx_index,
            index_in_tx=ix_index,
            signature=str(signature),
            was_successful=was_successful,
            mint_in=mint_in,
            mint_out=mint_out,
            amount_in=diff_from,
            amount_out=diff_to,
            limit_amount=limit_amount,
            limit_side=limit_side,
            post_pool_balance_mint_in=int(pool_in_balance),
            post_pool_balance_mint_out=int(pool_out_balance),
        )
    except Exception as e:
        logger.error(f"Error processing UiCompiledInstruction: {e}")
        return None

def change_direction(meta: UiTransactionStatusMeta, pool_from_index: int, pool_to_index: int) -> BalanceDiff:
    """
    Returns True if the direction of the swap should be changed.
    """
    if not meta.pre_token_balances or not meta.post_token_balances:
        raise ValueError("No pre or post token balances")

    diff_from = 0
    diff_to = 0

    # Find matching balances for pool_from
    for pre_balance in meta.pre_token_balances:
        if pre_balance.account_index == pool_from_index:
            for post_balance in meta.post_token_balances:
                if post_balance.account_index == pool_from_index:
                    diff_from = balance_diff(pre_balance, post_balance)
                    break
            break

    # Find matching balances for pool_to
    for pre_balance in meta.pre_token_balances:
        if pre_balance.account_index == pool_to_index:
            for post_balance in meta.post_token_balances:
                if post_balance.account_index == pool_to_index:
                    diff_to = balance_diff(pre_balance, post_balance)
                    break
            break

    return (abs(diff_from), abs(diff_to), diff_from < 0 and diff_to > 0)

def balance_diff(pre_balance: UiTransactionTokenBalance, post_balance: UiTransactionTokenBalance) -> int:
    """
    Returns the difference between the pre and post balance.
    """
    return int(post_balance.ui_token_amount.amount) - int(pre_balance.ui_token_amount.amount)

def get_pool_balances(meta: UiTransactionStatusMeta, pool_from_index: int, pool_to_index: int) -> PoolBalances:
    """
    Returns the balances of the pool_from and pool_to.
    """
    if not meta.post_token_balances:
        raise ValueError("No post token balances available")

    pool_from_balance = None
    pool_to_balance = None

    for balance in meta.post_token_balances:
        if balance.account_index == pool_from_index:
            pool_from_balance = balance.ui_token_amount.amount
        elif balance.account_index == pool_to_index:
            pool_to_balance = balance.ui_token_amount.amount

    if pool_from_balance is None or pool_to_balance is None:
        raise ValueError("Pool from index or pool to index not found in post token balances")

    return int(pool_from_balance), int(pool_to_balance)
