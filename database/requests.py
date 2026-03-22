"""Запросы к базе данных."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Awaitable, Callable, Iterable, Optional, Tuple, Dict, List, Set, TypeVar

from sqlalchemy import func, select
from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError

from database.models import Account, Token, Orders, Unit, Subscribe, Franchisee, Settings
from functions.shared.i18n import normalize_language

if TYPE_CHECKING:
    from resources.database import DatabaseResource


TOKEN_APPLICATION_ID = 1
DB_RETRY_ATTEMPTS = 3
LOGGER = logging.getLogger(__name__)
T = TypeVar("T")


async def _run_with_retry(
    *,
    operation_name: str,
    context: str,
    action: Callable[[], Awaitable[T]],
) -> T:
    """Выполняет DB-операцию с повтором на временных ошибках."""
    last_error: Optional[BaseException] = None

    for attempt in range(1, DB_RETRY_ATTEMPTS + 1):
        try:
            return await action()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if not _is_retryable_db_error(exc) or attempt >= DB_RETRY_ATTEMPTS:
                LOGGER.exception(
                    "db request failed: operation=%s context=%s attempt=%s/%s error=%s",
                    operation_name,
                    context,
                    attempt,
                    DB_RETRY_ATTEMPTS,
                    exc,
                )
                raise

            delay = _retry_delay(attempt)
            LOGGER.warning(
                "db transient error: operation=%s context=%s attempt=%s/%s retry_in=%.1fs error=%s",
                operation_name,
                context,
                attempt,
                DB_RETRY_ATTEMPTS,
                delay,
                exc,
            )
            await asyncio.sleep(delay)

    assert last_error is not None
    raise last_error


def _retry_delay(attempt: int) -> float:
    """Возвращает backoff-задержку для повторного DB-запроса."""
    return min(0.5 * (2 ** (attempt - 1)), 2.0)


def _is_retryable_db_error(exc: BaseException) -> bool:
    """Определяет, стоит ли повторять запрос после текущей ошибки."""
    if isinstance(exc, (ConnectionError, OSError, OperationalError, InterfaceError)):
        return True
    if isinstance(exc, DBAPIError):
        return exc.connection_invalidated or isinstance(exc.orig, (ConnectionError, OSError))

    message = str(exc).lower()
    retryable_markers = (
        "unexpected connection_lost() call",
        "connection reset by peer",
        "connection refused",
        "connection is closed",
        "server closed the connection unexpectedly",
        "terminating connection due to administrator command",
        "ssl",
        "timeout",
    )
    return any(marker in message for marker in retryable_markers)


async def load_account_and_token(
    db: DatabaseResource,
    account_id: int,
) -> Tuple[Optional[Account], Optional[Token]]:
    """Возвращает аккаунт и актуальный токен по account_id."""
    async def _action() -> Tuple[Optional[Account], Optional[Token]]:
        """Читает аккаунт и последний токен в одной DB-сессии."""
        sessionmaker = db.get_sessionmaker()
        async with sessionmaker() as session:
            account_stmt = select(Account).where(
                Account.id == account_id,
                Account.active.is_not(False),
            )
            account = (await session.execute(account_stmt)).scalar_one_or_none()
            if account is None:
                return None, None

            token_stmt = (
                select(Token)
                .where(
                    Token.account_id == account_id,
                    Token.application_id == TOKEN_APPLICATION_ID,
                )
                .order_by(Token.last_update.desc(), Token.id.desc())
                .limit(1)
            )
            token = (await session.execute(token_stmt)).scalar_one_or_none()
            return account, token

    return await _run_with_retry(
        operation_name="load_account_and_token",
        context=f"account_id={account_id}",
        action=_action,
    )


@dataclass(frozen=True)
class ReportSubscription:
    """Подписка на отчет по конкретной точке."""

    unit_uuid: str
    account_id: Optional[int]
    chat_id: int
    topic_id: Optional[int]
    language: str = "ru"
    params: Optional[str] = None
    post: Optional[str] = None


CouriersSubscription = ReportSubscription


async def select_report_subscriptions(
    db: DatabaseResource,
    post: str,
) -> List[ReportSubscription]:
    """Возвращает список подписок по post с разворотом uuids."""
    async def _action() -> List[ReportSubscription]:
        """Разворачивает записи orders в список подписок по заведениям."""
        sessionmaker = db.get_sessionmaker()
        subscriptions: List[ReportSubscription] = []
        async with sessionmaker() as session:
            orders_stmt = (
                select(Orders)
                .join(Account, Orders.account_id == Account.id)
                .where(
                    Orders.post == post,
                    Account.active.is_not(False),
                )
            )
            orders = (await session.execute(orders_stmt)).scalars().all()
            fallback_account_ids_by_chat = _extract_fallback_account_ids_by_chat(orders)
            languages_by_chat = await _load_chat_languages_in_session(
                session,
                {order.chat_id for order in orders},
                fallback_account_ids_by_chat=fallback_account_ids_by_chat,
            )
            for order in orders:
                if not order.uuids:
                    continue
                for unit_uuid in order.uuids:
                    if not unit_uuid:
                        continue
                    subscriptions.append(
                        ReportSubscription(
                            unit_uuid=unit_uuid,
                            account_id=order.account_id,
                            chat_id=order.chat_id,
                            topic_id=order.topic_id,
                            language=languages_by_chat.get(order.chat_id, "ru"),
                            params=order.params,
                            post=post,
                        )
                    )
        return subscriptions

    return await _run_with_retry(
        operation_name="select_report_subscriptions",
        context=f"post={post}",
        action=_action,
    )


async def select_couriers_subscriptions(db: DatabaseResource) -> List[ReportSubscription]:
    """Возвращает список подписок couriers с разворотом uuids."""
    return await select_report_subscriptions(db, "couriers")


async def select_report_subscriptions_many(
    db: DatabaseResource,
    posts: Iterable[str],
) -> List[ReportSubscription]:
    """Возвращает список подписок по нескольким post за один запрос."""
    post_list = [post for post in posts if post]
    if not post_list:
        return []

    async def _action() -> List[ReportSubscription]:
        """Разворачивает записи orders в список подписок по наборам post и uuids."""
        sessionmaker = db.get_sessionmaker()
        subscriptions: List[ReportSubscription] = []
        async with sessionmaker() as session:
            orders_stmt = (
                select(Orders)
                .join(Account, Orders.account_id == Account.id)
                .where(
                    Orders.post.in_(post_list),
                    Account.active.is_not(False),
                )
            )
            orders = (await session.execute(orders_stmt)).scalars().all()
            fallback_account_ids_by_chat = _extract_fallback_account_ids_by_chat(orders)
            languages_by_chat = await _load_chat_languages_in_session(
                session,
                {order.chat_id for order in orders},
                fallback_account_ids_by_chat=fallback_account_ids_by_chat,
            )
            for order in orders:
                if not order.uuids:
                    continue
                for unit_uuid in order.uuids:
                    if not unit_uuid:
                        continue
                    subscriptions.append(
                        ReportSubscription(
                            unit_uuid=unit_uuid,
                            account_id=order.account_id,
                            chat_id=order.chat_id,
                            topic_id=order.topic_id,
                            language=languages_by_chat.get(order.chat_id, "ru"),
                            params=order.params,
                            post=order.post,
                        )
                    )
        return subscriptions

    return await _run_with_retry(
        operation_name="select_report_subscriptions_many",
        context=f"posts={','.join(post_list)}",
        action=_action,
    )


async def _load_chat_languages_in_session(
    session,
    chat_ids: Iterable[int],
    *,
    fallback_account_ids_by_chat: Optional[Dict[int, int]] = None,
) -> Dict[int, str]:
    """Читает языки чатов внутри уже открытой DB-сессии."""
    chat_id_list = [chat_id for chat_id in chat_ids if chat_id]
    if not chat_id_list:
        return {}

    stmt = (
        select(Settings.chat_id, Settings.country)
        .where(Settings.chat_id.in_(chat_id_list))
        .order_by(Settings.id.desc())
    )
    rows = (await session.execute(stmt)).all()

    result: Dict[int, str] = {}
    for chat_id, language in rows:
        if chat_id not in result:
            result[int(chat_id)] = normalize_language(language)

    missing_chat_ids = [
        int(chat_id)
        for chat_id in chat_id_list
        if int(chat_id) not in result
    ]
    if missing_chat_ids and fallback_account_ids_by_chat:
        fallback_languages = await _load_fallback_chat_languages_by_account_in_session(
            session,
            missing_chat_ids,
            fallback_account_ids_by_chat,
        )
        result.update(fallback_languages)
    return result


async def load_chat_languages(
    db: DatabaseResource,
    chat_ids: Iterable[int],
    *,
    fallback_account_ids_by_chat: Optional[Dict[int, int]] = None,
) -> Dict[int, str]:
    """Возвращает языки интерфейса по chat_id."""
    chat_id_list = [chat_id for chat_id in chat_ids if chat_id]
    if not chat_id_list:
        return {}

    async def _action() -> Dict[int, str]:
        sessionmaker = db.get_sessionmaker()
        async with sessionmaker() as session:
            return await _load_chat_languages_in_session(
                session,
                chat_id_list,
                fallback_account_ids_by_chat=fallback_account_ids_by_chat,
            )

    return await _run_with_retry(
        operation_name="load_chat_languages",
        context=f"chat_ids={len(chat_id_list)}",
        action=_action,
    )


def _extract_fallback_account_ids_by_chat(orders: Iterable[Orders]) -> Dict[int, int]:
    """Выбирает account_id для fallback-языка по chat_id, если settings еще не заполнены."""
    result: Dict[int, int] = {}
    for order in orders:
        if not order.chat_id or order.account_id is None:
            continue
        result.setdefault(int(order.chat_id), int(order.account_id))
    return result


def _infer_language_from_account_properties(properties: object) -> Optional[str]:
    """Выводит базовый язык из properties аккаунта (`io/...` -> `ru`, `com/...` -> `en`)."""
    raw = str(properties or "").strip().strip("/").lower()
    if not raw:
        return None

    parts = [part for part in raw.split("/") if part]
    if not parts:
        return None
    if parts[0] == "com" or parts[-1] in {"en", "us", "gb", "com"}:
        return "en"
    if parts[0] == "io" or parts[-1] in {"ru", "kz", "by", "uz"}:
        return "ru"
    return None


async def _load_fallback_chat_languages_by_account_in_session(
    session,
    missing_chat_ids: Iterable[int],
    fallback_account_ids_by_chat: Dict[int, int],
) -> Dict[int, str]:
    """Достраивает язык чата через account.properties, если в special.settings записи еще нет."""
    chat_ids = [int(chat_id) for chat_id in missing_chat_ids if int(chat_id) in fallback_account_ids_by_chat]
    if not chat_ids:
        return {}

    account_ids = sorted({fallback_account_ids_by_chat[chat_id] for chat_id in chat_ids})
    stmt = select(Account.id, Account.properties).where(Account.id.in_(account_ids))
    rows = (await session.execute(stmt)).all()
    properties_by_account = {int(account_id): properties for account_id, properties in rows}

    result: Dict[int, str] = {}
    for chat_id in chat_ids:
        account_id = fallback_account_ids_by_chat.get(chat_id)
        if account_id is None:
            continue
        language = _infer_language_from_account_properties(properties_by_account.get(account_id))
        if language:
            result[chat_id] = normalize_language(language)
    return result


async def load_units_by_uuids(db: DatabaseResource, uuids: Iterable[str]) -> Dict[str, Unit]:
    """Загружает точки по uuid и возвращает словарь uuid -> Unit."""
    uuid_list = [uuid for uuid in uuids if uuid]
    if not uuid_list:
        return {}

    async def _action() -> Dict[str, Unit]:
        """Читает набор заведений одним bulk-запросом."""
        sessionmaker = db.get_sessionmaker()
        async with sessionmaker() as session:
            rows_stmt = select(Unit).where(Unit.uuid.in_(uuid_list))
            rows = (await session.execute(rows_stmt)).scalars().all()
            return {row.uuid: row for row in rows}

    return await _run_with_retry(
        operation_name="load_units_by_uuids",
        context=f"uuids={len(uuid_list)}",
        action=_action,
    )


async def load_franchisee_group_uuids_by_unit(
    db: DatabaseResource,
    uuids: Iterable[str],
) -> Dict[str, str]:
    """Возвращает group_uuid франчайзи по unit uuid для сетевой агрегации отчетов."""
    uuid_list = [uuid for uuid in uuids if uuid]
    if not uuid_list:
        return {}

    async def _action() -> Dict[str, str]:
        """Читает group_uuid для набора точек одним bulk-запросом."""
        sessionmaker = db.get_sessionmaker()
        async with sessionmaker() as session:
            rows_stmt = select(Franchisee.uuid, Franchisee.group_uuid).where(Franchisee.uuid.in_(uuid_list))
            rows = (await session.execute(rows_stmt)).all()
            return {uuid: group_uuid for uuid, group_uuid in rows if uuid and group_uuid}

    return await _run_with_retry(
        operation_name="load_franchisee_group_uuids_by_unit",
        context=f"uuids={len(uuid_list)}",
        action=_action,
    )


async def select_account_ids_for_units(
    db: DatabaseResource,
    unit_uuids: Iterable[str],
    *,
    account_ids_filter_by_unit: Optional[Dict[str, Iterable[int]]] = None,
) -> Dict[str, List[int]]:
    """Возвращает account_id по всем unit_uuid одним bulk-запросом."""
    unit_uuid_list = [unit_uuid for unit_uuid in unit_uuids if unit_uuid]
    if not unit_uuid_list:
        return {}

    allowed_accounts_by_unit = {
        unit_uuid: set(account_ids)
        for unit_uuid, account_ids in (account_ids_filter_by_unit or {}).items()
        if account_ids
    }
    all_allowed_accounts = sorted(
        {
            account_id
            for account_ids in allowed_accounts_by_unit.values()
            for account_id in account_ids
        }
    )

    async def _action() -> Dict[str, List[int]]:
        """Подбирает подходящие account_id для каждого unit_uuid."""
        sessionmaker = db.get_sessionmaker()
        async with sessionmaker() as session:
            query = (
                select(Subscribe.uuid, Token.account_id, Token.last_update, Token.id)
                .join(Token, Subscribe.token_id == Token.id)
                .join(Account, Token.account_id == Account.id)
                .where(
                    Subscribe.uuid.in_(unit_uuid_list),
                    Token.application_id == TOKEN_APPLICATION_ID,
                    Account.active.is_not(False),
                )
                .order_by(Subscribe.uuid, Token.last_update.desc(), Token.id.desc())
            )
            if all_allowed_accounts:
                query = query.where(Token.account_id.in_(all_allowed_accounts))

            rows = (await session.execute(query)).all()

            ordered_by_unit: Dict[str, List[int]] = {unit_uuid: [] for unit_uuid in unit_uuid_list}
            seen_by_unit: Dict[str, Set[int]] = {unit_uuid: set() for unit_uuid in unit_uuid_list}

            for unit_uuid, account_id, _, _ in rows:
                if account_id is None:
                    continue
                allowed_accounts = allowed_accounts_by_unit.get(unit_uuid)
                if allowed_accounts is not None and account_id not in allowed_accounts:
                    continue
                if account_id in seen_by_unit[unit_uuid]:
                    continue
                ordered_by_unit[unit_uuid].append(account_id)
                seen_by_unit[unit_uuid].add(account_id)

            return ordered_by_unit

    return await _run_with_retry(
        operation_name="select_account_ids_for_units",
        context=f"units={len(unit_uuid_list)}",
        action=_action,
    )


async def select_account_ids_for_unit(
    db: DatabaseResource,
    unit_uuid: str,
    *,
    account_ids_filter: Optional[Iterable[int]] = None,
) -> List[int]:
    """Возвращает список account_id по подпискам на unit_uuid, упорядоченный по свежести токена."""
    bulk_result = await select_account_ids_for_units(
        db,
        [unit_uuid],
        account_ids_filter_by_unit={unit_uuid: list(account_ids_filter or [])},
    )
    return bulk_result.get(unit_uuid, [])


async def load_accounts_and_tokens_bulk(
    db: DatabaseResource,
    account_ids: Iterable[int],
) -> Dict[int, Tuple[Optional[Account], Optional[Token]]]:
    """Возвращает latest token и account для всех account_id одним bulk-запросом."""
    account_id_list = [account_id for account_id in account_ids if account_id is not None]
    if not account_id_list:
        return {}

    async def _action() -> Dict[int, Tuple[Optional[Account], Optional[Token]]]:
        """Читает аккаунты и последние токены для списка account_id."""
        sessionmaker = db.get_sessionmaker()
        async with sessionmaker() as session:
            account_stmt = select(Account).where(
                Account.id.in_(account_id_list),
                Account.active.is_not(False),
            )
            accounts = (await session.execute(account_stmt)).scalars().all()
            active_account_ids = [account.id for account in accounts]

            if not active_account_ids:
                return {
                    account_id: (None, None)
                    for account_id in account_id_list
                }

            ranked_tokens = (
                select(
                    Token.id.label("token_id"),
                    Token.account_id.label("account_id"),
                    func.row_number()
                    .over(
                        partition_by=Token.account_id,
                        order_by=(Token.last_update.desc(), Token.id.desc()),
                    )
                    .label("rn"),
                )
                .where(
                    Token.account_id.in_(active_account_ids),
                    Token.application_id == TOKEN_APPLICATION_ID,
                )
                .subquery()
            )

            token_stmt = (
                select(Token)
                .join(ranked_tokens, Token.id == ranked_tokens.c.token_id)
                .where(ranked_tokens.c.rn == 1)
            )
            tokens = (await session.execute(token_stmt)).scalars().all()

            accounts_by_id = {account.id: account for account in accounts}
            tokens_by_account_id = {
                token.account_id: token
                for token in tokens
                if token.account_id is not None
            }

            return {
                account_id: (
                    accounts_by_id.get(account_id),
                    tokens_by_account_id.get(account_id),
                )
                for account_id in account_id_list
            }

    return await _run_with_retry(
        operation_name="load_accounts_and_tokens_bulk",
        context=f"account_ids={len(account_id_list)}",
        action=_action,
    )
