# app/seed.py
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

# ── Services ────────────────────────────────────────────────────────────────
PRESTATION_SERVICES = [
    ("تحديث بطاقة الشفاء", 10),
    ("طلب بطاقة الشفاء أو نسخة منها", 10),
    ("طلب استرجاع: توقف عن العمل، إجازة الأمومة، منتجات صيدلانية، رأس المال عند الوفاة، خدمات طبية", 15),
    ("طلب تمديد إجازة الأمومة", 10),
    ("تحديث ملف المؤمن عليه الاجتماعي", 12),
    ("فتح حقوق المؤمن عليه الاجتماعي وأفراد عائلته", 15),
    ("طلب شهادة انتساب أو عدم انتساب", 8),
    ("طلب تغطية لتجهيزات طبية أو علاج حراري", 12),
    ("إبلاغ عن حادث عمل", 10),
]

MEDICAL_SERVICES = [
    ("فحص طبي بعد توقف عن العمل", 8),
    ("تمديد إجازة الأمومة", 8),
    ("فحص قبلي", 6),
    ("استرجاع تكاليف العلاج الطبي", 10),
    ("طلبات أخرى مرتبطة بالمتابعة الطبية", 10),
]

PREFIXES = ["A","B","C","D","E","F","G","H","I","J","K","L","M","N"]

AGENTS = [
    ("وكيل 1",  ServiceCategory.prestation, "تحديث بطاقة الشفاء", "G01"),
    ("وكيل 2",  ServiceCategory.prestation, "طلب بطاقة الشفاء أو نسخة منها", "G02"),
    ("وكيل 3",  ServiceCategory.prestation,
     "طلب استرجاع: توقف عن العمل، إجازة الأمومة، منتجات صيدلانية، رأس المال عند الوفاة، خدمات طبية", "G03"),
    ("وكيل 4",  ServiceCategory.prestation, "طلب تمديد إجازة الأمومة", "G04"),
    ("وكيل 5",  ServiceCategory.prestation, "تحديث ملف المؤمن عليه الاجتماعي", "G05"),
    ("وكيل 6",  ServiceCategory.prestation, "فتح حقوق المؤمن عليه الاجتماعي وأفراد عائلته", "G06"),
    ("وكيل 7",  ServiceCategory.prestation, "طلب شهادة انتساب أو عدم انتساب", "G07"),
    ("وكيل 8",  ServiceCategory.prestation, "طلب تغطية لتجهيزات طبية أو علاج حراري", "G08"),
    ("وكيل 9",  ServiceCategory.prestation, "إبلاغ عن حادث عمل", "G09"),

    ("وكيل 10", ServiceCategory.medical, "فحص طبي بعد توقف عن العمل", "G10"),
    ("وكيل 11", ServiceCategory.medical, "تمديد إجازة الأمومة", "G11"),
    ("وكيل 12", ServiceCategory.medical, "فحص قبلي", "G12"),
    ("وكيل 13", ServiceCategory.medical, "استرجاع تكاليف العلاج الطبي", "G13"),
    ("وكيل 14", ServiceCategory.medical, "طلبات أخرى مرتبطة بالمتابعة الطبية", "G14"),
]


async def seed():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with AsyncSession() as db:


            # ── ADMIN USER (ADD HERE FIRST) ─────────────────────
        ADMIN_USERNAME = "admin"
        ADMIN_PASSWORD = "admin1234"

        result = await db.execute(
            select(User).where(User.username == ADMIN_USERNAME)
        )
        admin = result.scalar_one_or_none()

        if not admin:
            admin = User(
                username=ADMIN_USERNAME,
                password=hash_password(ADMIN_PASSWORD),
                role=UserRole.admin
            )
            db.add(admin)
            await db.flush()

            print("✅ Admin created")
        else:
            print("ℹ️ Admin already exists")

        # ─────────────────────────────────────────────
        # SERVICES
        # ─────────────────────────────────────────────
        all_services = []
        idx = 0

        for name, avg_time in PRESTATION_SERVICES:
            r = await db.execute(select(Service).where(Service.name == name))
            s = r.scalar_one_or_none()

            if not s:
                s = Service(name=name, category=ServiceCategory.prestation, avg_time_min=avg_time)
                db.add(s)

            all_services.append((s, idx))
            idx += 1

        for name, avg_time in MEDICAL_SERVICES:
            r = await db.execute(select(Service).where(Service.name == name))
            s = r.scalar_one_or_none()

            if not s:
                s = Service(name=name, category=ServiceCategory.medical, avg_time_min=avg_time)
                db.add(s)

            all_services.append((s, idx))
            idx += 1

        await db.flush()

        # ─────────────────────────────────────────────
        # QUEUES
        # ─────────────────────────────────────────────
        for service, i in all_services:
            r = await db.execute(select(Queue).where(Queue.service_id == service.id))
            if r.scalar_one_or_none():
                continue

            db.add(
                Queue(
                    service_id=service.id,
                    prefix=PREFIXES[i % len(PREFIXES)],
                    counter=0
                )
            )

        await db.flush()

        # ─────────────────────────────────────────────
        # AGENTS (FIXED HERE 🔥)
        # ─────────────────────────────────────────────
        for name, category, assigned_service, queue_number in AGENTS:

            r = await db.execute(select(User).where(User.username == name))
            if r.scalar_one_or_none():
                continue

            user = User(
                username=name,
                password=hash_password("cnas1234"),
                role=UserRole.agent
            )
            db.add(user)
            await db.flush()

            # 🔥 FIND SERVICE BY NAME → GET ID
            service_obj = await db.execute(
                select(Service).where(Service.name == assigned_service)
            )
            service = service_obj.scalar_one_or_none()

            db.add(
                Agent(
                    user_id=user.id,
                    name=name,
                    category=category,
                    assigned_service=assigned_service,   # keep (for UI)
                    assigned_service_id=service.id if service else None,  # FIXED
                    queue_number=queue_number,
                    structure="الصندوق الوطني للتأمينات الاجتماعية",
                )
            )

        await db.commit()

    print("✅ Database seeded successfully!")
    print("Multi-mode now correctly uses service_id routing")


if __name__ == "__main__":
    asyncio.run(seed())