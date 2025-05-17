from sqlmodel import (
    Column,
    Field,
    Float,
    Session,
    SQLModel,
    String,
    create_engine,
    select,
)


class BaseModel(SQLModel):
    rid: str = Field()
    ver: int = Field()
    hash: str = Field()


class DataRecord(BaseModel, table=True):
    __tablename__ = "syf-test"

    record_id: str = Field(primary_key=True)
    数据名称: str = Field(sa_column=Column(String))
    数据链接: str = Field(sa_column=Column(String))
    数据简介: str = Field(sa_column=Column(String))
    一级领域: str = Field(sa_column=Column(String))
    二级领域: str = Field(sa_column=Column(String))
    三级领域: str = Field(sa_column=Column(String))
    数据类型: str = Field(sa_column=Column(String))
    国家地区: str = Field(alias="国家/地区", sa_column=Column("国家/地区", String))
    语言语种: str = Field(alias="语言/语种", sa_column=Column("语言/语种", String))
    模态: float = Field(sa_column=Column(Float))
    预估大小: float = Field(alias="预估大小（GB）", sa_column=Column("预估大小（GB）", Float))
    数据组织方: float = Field(sa_column=Column(Float))
    数据发布方: float = Field(sa_column=Column(Float))
    数据更新时间: float = Field(sa_column=Column(Float))
    访问权限: float = Field(sa_column=Column(Float))
    获取状态: float = Field(sa_column=Column(Float))
    线索来源类型: float = Field(sa_column=Column(Float))
    线索来源合作方: float = Field(alias="线索来源-合作方", sa_column=Column("线索来源-合作方", Float))
    线索来源事件触发: float = Field(alias="线索来源-事件触发", sa_column=Column("线索来源-事件触发", Float))
    线索提供单位: float = Field(sa_column=Column(Float))
    数据子领域: float = Field(sa_column=Column(Float))
    数据流程: str = Field(sa_column=Column(String))


class FeishuDocSync:
    def __init__(self, db_url: str):
        self.engine = create_engine(db_url)
        SQLModel.metadata.create_all(self.engine)

    def compare_records(self, existing: DataRecord, new: DataRecord) -> Dict[str, Any]:
        """比较两个记录的差异"""
        changes = {}
        for field in new.__fields__:
            if field == "record_id":
                continue

            old_value = getattr(existing, field)
            new_value = getattr(new, field)

            # 处理浮点数比较
            if isinstance(old_value, float) and isinstance(new_value, float):
                if abs(old_value - new_value) > 1e-6:  # 使用小的误差范围
                    changes[field] = new_value
            # 处理字符串比较
            elif old_value != new_value:
                print(f"字段 {field} 的值从 {old_value} 变为 {new_value}")
                changes[field] = new_value

        return changes

    def sync_to_db(self, doc_data: List[Dict[str, Any]]):
        """同步数据到数据库"""
        try:
            with Session(self.engine) as session:
                # 获取所有现有记录
                existing_records = session.exec(select(DataRecord)).all()
                existing_dict = {record.record_id: record for record in existing_records}

                # 处理每条文档数据
                for doc_record in doc_data:
                    record_id = doc_record.get("record_id")
                    if not record_id:
                        continue

                    # 创建新的记录对象
                    new_record = DataRecord(**doc_record)
                    # # 打印新记录的所有字段
                    print(f"\n新记录 {record_id} 的字段值:")
                    for field in new_record.__fields__:
                        value = getattr(new_record, field)
                        print(f"{field}: {value}")
                    print()

                    if record_id in existing_dict:
                        # 比较并更新变化的字段
                        existing_record = existing_dict[record_id]
                        changes = self.compare_records(existing_record, new_record)

                        if changes:
                            print(f"记录 {record_id} 有以下字段需要更新: {list(changes.keys())}")
                            for field, value in changes.items():
                                setattr(existing_record, field, value)
                            session.add(existing_record)
                    else:
                        # 添加新记录
                        print(f"添加新记录: {record_id}")
                        session.add(new_record)

                session.commit()
                print("数据同步完成")

        except Exception as e:
            print(f"同步数据到数据库失败: {str(e)}")
            raise


for field in sync.bi_fields.values():
    name, typ = field["field_name"], field["type"]
    if typ in [1001, 1002, 1003, 1004, 1005, 11, 20, 24]:
        # 1001: timestamp::created_time
        # 1002: timestamp::modified_time
        # 1003: user::created_user
        # 1004: user::modified_user
        # 1005: number/str::auto_increment_number
        # 11: user  field["property"]["multiple"]
        # 20: formula
        # 24: stage
        continue
    if typ in [1, 3, 4, 15]:
        # 1: str
        # 3: str::single-select
        # 4: str::multi-select
        # 15: str::url
        a = str
    elif typ == 2:
        # 2: number
        formatter = field["property"]["formatter"]
        if "0.0" in formatter:
            a = float
        else:
            a = int
    else:
        raise ValueError(f"不支持的类型: {typ}")

    print(name, a)
    if typ in [3, 4]:
        options = field["property"]["options"]
        for option in options:
            print("    ", option["name"])
