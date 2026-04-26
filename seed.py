"""
Seed script — exact services from SystemContext.tsx
Usage: python seed.py
"""

import asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.config import settings
from app.database import Base
from app.models.models import User, Agent, Service, Queue, UserRole, ServiceCategory
from app.core.auth import hash_password

engine = create_async_engine(settings.DATABASE_URL, echo=False)
AsyncSession = async_sessionmaker(engine, expire_on_commit=False)

# ── Exact copy from SystemContext.tsx ─────────────────────────────────────────
PRESTATION_SERVICES = [
    ("تحديث بطاقة الشفاء",                                                                                          10),
    ("طلب بطاقة الشفاء أو نسخة منها",                                                                               10),
    ("طلب استرجاع: توقف عن العمل، إجازة الأمومة، منتجات صيدلانية، رأس المال عند الوفاة، خدمات طبية",              15),
    ("طلب تمديد إجازة الأمومة",                                                                                      10),
    ("تحديث ملف المؤمن عليه الاجتماعي",                                                                             12),
    ("فتح حقوق المؤمن عليه الاجتماعي وأفراد عائلته",                                                               15),
    ("طلب شهادة انتساب أو عدم انتساب",                                                                              8),
    ("طلب تغطية لتجهيزات طبية أو علاج حراري",                                                                       12),
    ("إبلاغ عن حادث عمل",                                                                                           10),
]

MEDICAL_SERVICES = [
    ("فحص طبي بعد توقف عن العمل",           8),
    ("تمديد إجازة الأمومة",                  8),
    ("فحص قبلي",                             6),
    ("استرجاع تكاليف العلاج الطبي",          10),
    ("طلبات أخرى مرتبطة بالمتابعة الطبية",  10),
]

PREFIXES = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N"]

# One agent per service for multi-desk mode
AGENTS = [
    # Prestation agents
    ("وكيل 1",  ServiceCategory.prestation, "تحديث بطاقة الشفاء",               "G01"),
    ("وكيل 2",  ServiceCategory.prestation, "طلب بطاقة الشفاء أو نسخة منها",    "G02"),
    ("وكيل 3",  ServiceCategory.prestation, "طلب استرجاع: توقف عن العمل، إجازة الأمومة، منتجات صيدلانية، رأس المال عند الوفاة، خدمات طبية", "G03"),
    ("وكيل 4",  ServiceCategory.prestation, "طلب تمديد إجازة الأمومة",           "G04"),
    ("وكيل 5",  ServiceCategory.prestation, "تحديث ملف المؤمن عليه الاجتماعي",  "G05"),
    ("وكيل 6",  ServiceCategory.prestation, "فتح حقوق المؤمن عليه الاجتماعي وأفراد عائلته", "G06"),
    ("وكيل 7",  ServiceCategory.prestation, "طلب شهادة انتساب أو عدم انتساب",   "G07"),
    ("وكيل 8",  ServiceCategory.prestation, "طلب تغطية لتجهيزات طبية أو علاج حراري", "G08"),
    ("وكيل 9",  ServiceCategory.prestation, "إبلاغ عن حادث عمل",                "G09"),
    # Medical agents
    ("وكيل 10", ServiceCategory.medical,    "فحص طبي بعد توقف عن العمل",        "G10"),
    ("وكيل 11", ServiceCategory.medical,    "تمديد إجازة الأمومة",               "G11"),
    ("وكيل 12", ServiceCategory.medical,    "فحص قبلي",                          "G12"),
    ("وكيل 13", ServiceCategory.medical,    "استرجاع تكاليف العلاج الطبي",       "G13"),
    ("وكيل 14", ServiceCategory.medical,    "طلبات أخرى مرتبطة بالمتابعة الطبية","G14"),
]


async def seed():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with AsyncSession() as db:

        # ── Services ──────────────────────────────────────────────────────────
        all_services = []
        idx = 0

        for name, avg_time in PRESTATION_SERVICES:
            r = await db.execute(select(Service).where(Service.name == name))
            s = r.scalar_one_or_none()
            if s:
                print(f"  skip service: {name[:30]}…")
            else:
                s = Service(name=name, category=ServiceCategory.prestation, avg_time_min=avg_time)
                db.add(s)
            all_services.append((s, idx))
            idx += 1

        for name, avg_time in MEDICAL_SERVICES:
            r = await db.execute(select(Service).where(Service.name == name))
            s = r.scalar_one_or_none()
            if s:
                print(f"  skip service: {name[:30]}…")
            else:
                s = Service(name=name, category=ServiceCategory.medical, avg_time_min=avg_time)
                db.add(s)
            all_services.append((s, idx))
            idx += 1

        await db.flush()
        print(f"✓ {len(PRESTATION_SERVICES)} prestation + {len(MEDICAL_SERVICES)} medical services")

        # ── Queues ────────────────────────────────────────────────────────────
        for service, i in all_services:
            r = await db.execute(select(Queue).where(Queue.service_id == service.id))
            if r.scalar_one_or_none():
                continue
            db.add(Queue(service_id=service.id, prefix=PREFIXES[i % len(PREFIXES)], counter=0))

        await db.flush()
        print("✓ Queues created")

        # ── Agents ────────────────────────────────────────────────────────────
        for name, category, assigned_service, queue_number in AGENTS:
            r = await db.execute(select(User).where(User.username == name))
            if r.scalar_one_or_none():
                print(f"  skip agent: {name}")
                continue

            user = User(username=name, password=hash_password("cnas1234"), role=UserRole.agent)
            db.add(user)
            await db.flush()

            db.add(Agent(
                user_id=user.id,
                name=name,
                category=category,
                assigned_service=assigned_service,
                queue_number=queue_number,
                structure="الصندوق الوطني للتأمينات الاجتماعية",
            ))

        await db.flush()
        print(f"✓ {len(AGENTS)} agents created")

        await db.commit()

    print()
    print("=" * 55)
    print("Database seeded successfully!")
    print("=" * 55)
    print()
    print("Admin      → admin / admin1234")
    print("Agents     → وكيل X / cnas1234")
    print()
    print("9 prestation services + 5 medical services")
    print("14 agents — one per service (multi-desk ready)")
    print("Single-desk also works: agents with no assignment handle all queues")


if __name__ == "__main__":
    asyncio.run(seed())
