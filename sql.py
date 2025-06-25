#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sqlite3
import logging
from typing import Any, Dict, List, Optional, Tuple, Union
from contextlib import contextmanager
import json
import os

class SQLiteDB:
    """
    SQLite数据库操作类
    提供完整的数据库操作功能，包括连接管理、CRUD操作、事务处理等
    """
    
    def __init__(self, db_path: str, timeout: float = 30.0, 
                 check_same_thread: bool = False):
        """
        初始化数据库连接
        
        Args:
            db_path: 数据库文件路径
            timeout: 连接超时时间
            check_same_thread: 是否检查同一线程
        """
        self.db_path = db_path
        self.timeout = timeout
        self.check_same_thread = check_same_thread
        self.connection = None
        self._setup_logging()
        
    def _setup_logging(self):
        """设置日志"""
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def connect(self) -> sqlite3.Connection:
        """
        创建数据库连接
        
        Returns:
            sqlite3.Connection: 数据库连接对象
        """
        try:
            if self.connection is None or self._is_connection_closed():
                self.connection = sqlite3.connect(
                    self.db_path,
                    timeout=self.timeout,
                    check_same_thread=self.check_same_thread
                )
                self.connection.row_factory = sqlite3.Row  # 使结果可以按列名访问
                self.logger.info(f"成功连接到数据库: {self.db_path}")
            return self.connection
        except sqlite3.Error as e:
            self.logger.error(f"连接数据库失败: {e}")
            raise
    
    def _is_connection_closed(self) -> bool:
        """检查连接是否已关闭"""
        try:
            self.connection.execute("SELECT 1")
            return False
        except (sqlite3.Error, AttributeError):
            return True
    
    def close(self):
        """关闭数据库连接"""
        if self.connection:
            try:
                self.connection.close()
                self.connection = None
                self.logger.info("数据库连接已关闭")
            except sqlite3.Error as e:
                self.logger.error(f"关闭数据库连接失败: {e}")
    
    def __enter__(self):
        """上下文管理器入口"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        if exc_type:
            self.rollback()
        self.close()
    
    @contextmanager
    def transaction(self):
        """
        事务上下文管理器
        
        Usage:
            with db.transaction():
                db.execute("INSERT INTO ...")
                db.execute("UPDATE ...")
        """
        conn = self.connect()
        try:
            yield conn
            conn.commit()
            self.logger.info("事务提交成功")
        except Exception as e:
            conn.rollback()
            self.logger.error(f"事务回滚: {e}")
            raise
    
    def execute(self, sql: str, params: Optional[Union[Tuple, Dict]] = None) -> sqlite3.Cursor:
        """
        执行SQL语句
        
        Args:
            sql: SQL语句
            params: 参数
            
        Returns:
            sqlite3.Cursor: 游标对象
        """
        conn = self.connect()
        try:
            if params:
                cursor = conn.execute(sql, params)
            else:
                cursor = conn.execute(sql)
            self.logger.debug(f"执行SQL: {sql}")
            return cursor
        except sqlite3.Error as e:
            self.logger.error(f"执行SQL失败: {sql}, 错误: {e}")
            raise
    
    def executemany(self, sql: str, params_list: List[Union[Tuple, Dict]]) -> sqlite3.Cursor:
        """
        批量执行SQL语句
        
        Args:
            sql: SQL语句
            params_list: 参数列表
            
        Returns:
            sqlite3.Cursor: 游标对象
        """
        conn = self.connect()
        try:
            cursor = conn.executemany(sql, params_list)
            self.logger.debug(f"批量执行SQL: {sql}, 记录数: {len(params_list)}")
            return cursor
        except sqlite3.Error as e:
            self.logger.error(f"批量执行SQL失败: {sql}, 错误: {e}")
            raise
    
    def commit(self):
        """提交事务"""
        if self.connection:
            try:
                self.connection.commit()
                self.logger.debug("事务已提交")
            except sqlite3.Error as e:
                self.logger.error(f"提交事务失败: {e}")
                raise
    
    def rollback(self):
        """回滚事务"""
        if self.connection:
            try:
                self.connection.rollback()
                self.logger.debug("事务已回滚")
            except sqlite3.Error as e:
                self.logger.error(f"回滚事务失败: {e}")
                raise
    
    def create_table(self, table_name: str, columns: Dict[str, str], 
                    if_not_exists: bool = True) -> bool:
        """
        创建表
        
        Args:
            table_name: 表名
            columns: 列定义字典 {'column_name': 'column_type'}
            if_not_exists: 是否使用IF NOT EXISTS
            
        Returns:
            bool: 是否成功
        """
        try:
            columns_def = ", ".join([f"{name} {type_}" for name, type_ in columns.items()])
            if_not_exists_clause = "IF NOT EXISTS " if if_not_exists else ""
            sql = f"CREATE TABLE {if_not_exists_clause}{table_name} ({columns_def})"
            
            self.execute(sql)
            self.commit()
            self.logger.info(f"表 {table_name} 创建成功")
            return True
        except sqlite3.Error as e:
            self.logger.error(f"创建表失败: {e}")
            return False
    
    def drop_table(self, table_name: str, if_exists: bool = True) -> bool:
        """
        删除表
        
        Args:
            table_name: 表名
            if_exists: 是否使用IF EXISTS
            
        Returns:
            bool: 是否成功
        """
        try:
            if_exists_clause = "IF EXISTS " if if_exists else ""
            sql = f"DROP TABLE {if_exists_clause}{table_name}"
            
            self.execute(sql)
            self.commit()
            self.logger.info(f"表 {table_name} 删除成功")
            return True
        except sqlite3.Error as e:
            self.logger.error(f"删除表失败: {e}")
            return False
    
    def insert(self, table_name: str, data: Dict[str, Any], 
               or_replace: bool = False) -> Optional[int]:
        """
        插入单条记录
        
        Args:
            table_name: 表名
            data: 数据字典
            or_replace: 是否使用INSERT OR REPLACE
            
        Returns:
            Optional[int]: 插入记录的rowid
        """
        try:
            columns = ", ".join(data.keys())
            placeholders = ", ".join(["?" for _ in data])
            replace_clause = "OR REPLACE " if or_replace else ""
            sql = f"INSERT {replace_clause}INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            cursor = self.execute(sql, tuple(data.values()))
            self.commit()
            row_id = cursor.lastrowid
            self.logger.info(f"插入记录成功, rowid: {row_id}")
            return row_id
        except sqlite3.Error as e:
            self.logger.error(f"插入记录失败: {e}")
            return None
    
    def insert_many(self, table_name: str, data_list: List[Dict[str, Any]], 
                   or_replace: bool = False) -> bool:
        """
        批量插入记录
        
        Args:
            table_name: 表名
            data_list: 数据字典列表
            or_replace: 是否使用INSERT OR REPLACE
            
        Returns:
            bool: 是否成功
        """
        if not data_list:
            return True
            
        try:
            # 使用第一条记录的键作为列名
            columns = ", ".join(data_list[0].keys())
            placeholders = ", ".join(["?" for _ in data_list[0]])
            replace_clause = "OR REPLACE " if or_replace else ""
            sql = f"INSERT {replace_clause}INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            # 转换为元组列表
            params_list = [tuple(data.values()) for data in data_list]
            
            with self.transaction():
                self.executemany(sql, params_list)
            
            self.logger.info(f"批量插入 {len(data_list)} 条记录成功")
            return True
        except sqlite3.Error as e:
            self.logger.error(f"批量插入记录失败: {e}")
            return False
    
    def select(self, table_name: str, columns: str = "*", 
              where: Optional[str] = None, params: Optional[Tuple] = None,
              order_by: Optional[str] = None, limit: Optional[int] = None,
              offset: Optional[int] = None) -> List[sqlite3.Row]:
        """
        查询记录
        
        Args:
            table_name: 表名
            columns: 查询列名
            where: WHERE条件
            params: WHERE条件参数
            order_by: 排序条件
            limit: 限制记录数
            offset: 偏移量
            
        Returns:
            List[sqlite3.Row]: 查询结果列表
        """
        try:
            sql = f"SELECT {columns} FROM {table_name}"
            
            if where:
                sql += f" WHERE {where}"
            if order_by:
                sql += f" ORDER BY {order_by}"
            if limit:
                sql += f" LIMIT {limit}"
            if offset:
                sql += f" OFFSET {offset}"
            
            cursor = self.execute(sql, params)
            results = cursor.fetchall()
            self.logger.debug(f"查询到 {len(results)} 条记录")
            return results
        except sqlite3.Error as e:
            self.logger.error(f"查询记录失败: {e}")
            return []
    
    def select_one(self, table_name: str, columns: str = "*",
                  where: Optional[str] = None, params: Optional[Tuple] = None) -> Optional[sqlite3.Row]:
        """
        查询单条记录
        
        Args:
            table_name: 表名
            columns: 查询列名
            where: WHERE条件
            params: WHERE条件参数
            
        Returns:
            Optional[sqlite3.Row]: 查询结果
        """
        results = self.select(table_name, columns, where, params, limit=1)
        return results[0] if results else None
    
    def update(self, table_name: str, data: Dict[str, Any], 
              where: str, where_params: Optional[Tuple] = None) -> int:
        """
        更新记录
        
        Args:
            table_name: 表名
            data: 更新数据字典
            where: WHERE条件
            where_params: WHERE条件参数
            
        Returns:
            int: 受影响的行数
        """
        try:
            set_clause = ", ".join([f"{key} = ?" for key in data.keys()])
            sql = f"UPDATE {table_name} SET {set_clause} WHERE {where}"
            
            # 合并参数
            params = list(data.values())
            if where_params:
                params.extend(where_params)
            
            cursor = self.execute(sql, tuple(params))
            self.commit()
            affected_rows = cursor.rowcount
            self.logger.info(f"更新 {affected_rows} 条记录")
            return affected_rows
        except sqlite3.Error as e:
            self.logger.error(f"更新记录失败: {e}")
            return 0
    
    def delete(self, table_name: str, where: str, 
              params: Optional[Tuple] = None) -> int:
        """
        删除记录
        
        Args:
            table_name: 表名
            where: WHERE条件
            params: WHERE条件参数
            
        Returns:
            int: 受影响的行数
        """
        try:
            sql = f"DELETE FROM {table_name} WHERE {where}"
            cursor = self.execute(sql, params)
            self.commit()
            affected_rows = cursor.rowcount
            self.logger.info(f"删除 {affected_rows} 条记录")
            return affected_rows
        except sqlite3.Error as e:
            self.logger.error(f"删除记录失败: {e}")
            return 0
    
    def count(self, table_name: str, where: Optional[str] = None, 
             params: Optional[Tuple] = None) -> int:
        """
        统计记录数
        
        Args:
            table_name: 表名
            where: WHERE条件
            params: WHERE条件参数
            
        Returns:
            int: 记录数
        """
        try:
            sql = f"SELECT COUNT(*) as count FROM {table_name}"
            if where:
                sql += f" WHERE {where}"
            
            cursor = self.execute(sql, params)
            result = cursor.fetchone()
            return result['count'] if result else 0
        except sqlite3.Error as e:
            self.logger.error(f"统计记录失败: {e}")
            return 0
    
    def exists(self, table_name: str, where: str, 
              params: Optional[Tuple] = None) -> bool:
        """
        检查记录是否存在
        
        Args:
            table_name: 表名
            where: WHERE条件
            params: WHERE条件参数
            
        Returns:
            bool: 是否存在
        """
        return self.count(table_name, where, params) > 0
    
    def get_table_info(self, table_name: str) -> List[sqlite3.Row]:
        """
        获取表结构信息
        
        Args:
            table_name: 表名
            
        Returns:
            List[sqlite3.Row]: 表结构信息
        """
        try:
            cursor = self.execute(f"PRAGMA table_info({table_name})")
            return cursor.fetchall()
        except sqlite3.Error as e:
            self.logger.error(f"获取表结构失败: {e}")
            return []
    
    def get_tables(self) -> List[str]:
        """
        获取所有表名
        
        Returns:
            List[str]: 表名列表
        """
        try:
            cursor = self.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
            return [row['name'] for row in cursor.fetchall()]
        except sqlite3.Error as e:
            self.logger.error(f"获取表列表失败: {e}")
            return []
    
    def backup(self, backup_path: str) -> bool:
        """
        备份数据库
        
        Args:
            backup_path: 备份文件路径
            
        Returns:
            bool: 是否成功
        """
        try:
            source = self.connect()
            backup_conn = sqlite3.connect(backup_path)
            source.backup(backup_conn)
            backup_conn.close()
            self.logger.info(f"数据库备份成功: {backup_path}")
            return True
        except sqlite3.Error as e:
            self.logger.error(f"数据库备份失败: {e}")
            return False
    
    def vacuum(self) -> bool:
        """
        优化数据库（VACUUM）
        
        Returns:
            bool: 是否成功
        """
        try:
            self.execute("VACUUM")
            self.logger.info("数据库优化完成")
            return True
        except sqlite3.Error as e:
            self.logger.error(f"数据库优化失败: {e}")
            return False
    
    def get_db_size(self) -> int:
        """
        获取数据库文件大小（字节）
        
        Returns:
            int: 文件大小
        """
        try:
            return os.path.getsize(self.db_path)
        except OSError as e:
            self.logger.error(f"获取数据库大小失败: {e}")
            return 0
    
    def export_to_json(self, table_name: str, file_path: str) -> bool:
        """
        导出表数据到JSON文件
        
        Args:
            table_name: 表名
            file_path: JSON文件路径
            
        Returns:
            bool: 是否成功
        """
        try:
            rows = self.select(table_name)
            data = [dict(row) for row in rows]
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"数据导出成功: {file_path}")
            return True
        except Exception as e:
            self.logger.error(f"数据导出失败: {e}")
            return False
    
    def import_from_json(self, table_name: str, file_path: str, 
                        or_replace: bool = False) -> bool:
        """
        从JSON文件导入数据
        
        Args:
            table_name: 表名
            file_path: JSON文件路径
            or_replace: 是否替换已存在的记录
            
        Returns:
            bool: 是否成功
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if data:
                return self.insert_many(table_name, data, or_replace)
            return True
        except Exception as e:
            self.logger.error(f"数据导入失败: {e}")
            return False


# 使用示例
def example_usage():
    """使用示例"""
    
    # 创建数据库实例
    db = SQLiteDB("example.db")
    
    try:
        # 使用上下文管理器
        with db:
            # 创建表
            columns = {
                "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
                "name": "TEXT NOT NULL",
                "age": "INTEGER",
                "email": "TEXT UNIQUE",
                "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            }
            db.create_table("users", columns)
            
            # 插入单条记录
            user_data = {
                "name": "张三",
                "age": 25,
                "email": "zhangsan@example.com"
            }
            row_id = db.insert("users", user_data)
            print(f"插入记录ID: {row_id}")
            
            # 批量插入
            users_data = [
                {"name": "李四", "age": 30, "email": "lisi@example.com"},
                {"name": "王五", "age": 28, "email": "wangwu@example.com"},
                {"name": "赵六", "age": 35, "email": "zhaoliu@example.com"}
            ]
            db.insert_many("users", users_data)
            
            # 查询所有记录
            all_users = db.select("users")
            print(f"所有用户: {len(all_users)} 条")
            for user in all_users:
                print(f"ID: {user['id']}, 姓名: {user['name']}, 年龄: {user['age']}")
            
            # 条件查询
            adult_users = db.select("users", where="age >= ?", params=(30,))
            print(f"成年用户: {len(adult_users)} 条")
            
            # 查询单条记录
            user = db.select_one("users", where="name = ?", params=("张三",))
            if user:
                print(f"找到用户: {user['name']}")
            
            # 更新记录
            affected = db.update("users", {"age": 26}, "name = ?", ("张三",))
            print(f"更新了 {affected} 条记录")
            
            # 统计记录数
            count = db.count("users")
            print(f"总用户数: {count}")
            
            # 检查是否存在
            exists = db.exists("users", "email = ?", ("zhangsan@example.com",))
            print(f"邮箱存在: {exists}")
            
            # 使用事务
            with db.transaction():
                db.insert("users", {"name": "事务用户1", "age": 20, "email": "tx1@example.com"})
                db.insert("users", {"name": "事务用户2", "age": 22, "email": "tx2@example.com"})
            
            # 获取表信息
            table_info = db.get_table_info("users")
            print("表结构:")
            for column in table_info:
                print(f"  {column['name']}: {column['type']}")
            
            # 导出数据
            db.export_to_json("users", "users_backup.json")
            
            # 备份数据库
            db.backup("example_backup.db")
            
        print("所有操作完成!")
        
    except Exception as e:
        print(f"操作失败: {e}")


if __name__ == "__main__":
    example_usage()