#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `zentity.core` module."""

from dataclasses import dataclass

from zentity.core import Repository, model

def test_repository_creates_as_expected(mocker):
    class MyFakeEntity:
        pass
    mocker.patch('records.Database')
    repository = Repository(MyFakeEntity)
    assert repository.model == MyFakeEntity
    assert repository.model_name == 'MyFakeEntity'
    assert repository.table_name == "my_fake_entity"

def test_repository_recognizes_table_name_option(mocker):
    class MyFakeEntity:
        table_name = "my_super_table_name"
    mocker.patch('records.Database')
    repository = Repository(MyFakeEntity)
    assert repository.model_name == 'MyFakeEntity'
    assert repository.table_name == "my_super_table_name"

def test_repository_last_id_returns_id(mocker):
    class MyFakeEntity:
        pass
    ID = 77
    Database = mocker.patch('records.Database')
    Database.return_value.query.return_value.all.return_value = [{'id': ID}]
    repository = Repository(MyFakeEntity)
    assert repository._last_id == ID

def test_repository_last_id_returns_none(mocker):
    class MyFakeEntity:
        pass
    Database = mocker.patch('records.Database')
    Database.return_value.query.return_value.all.return_value = []
    repository = Repository(MyFakeEntity)
    assert repository._last_id is None

def test_repository_create_generate_correct_sql(mocker):
    PARAMS_DICT = {'id':1, 'title': 'essai', 'content': 'lorem ipsum'}
    class MyFakeEntity:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
    Database = mocker.patch('records.Database')
    repository = Repository(MyFakeEntity)
    instance = repository.create(**PARAMS_DICT)
    args, kwargs = Database.return_value.query.call_args
    assert 'INSERT INTO my_fake_entity(id, title, content)' in args[0]
    assert 'VALUES (:id, :title, :content)' in args[0]

def test_repository_create_returns_instance(mocker):
    PARAMS_DICT = {'id':1, 'title': 'essai', 'content': 'lorem ipsum'}
    class MyFakeEntity:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
    Database = mocker.patch('records.Database')
    repository = Repository(MyFakeEntity)
    instance = repository.create(**PARAMS_DICT)
    assert isinstance(instance, MyFakeEntity)

def test_repository_get_or_create_generate_correct_getallby_sql(mocker):
    PARAMS_DICT = {'id':1, 'title': 'essai', 'content': 'lorem ipsum'}
    class MyFakeEntity:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
    Database = mocker.patch('records.Database')
    Database.return_value.query.return_value.all.return_value = [PARAMS_DICT]
    repository = Repository(MyFakeEntity)
    instance = repository.get_or_create(**PARAMS_DICT)
    args, kwargs = Database.return_value.query.call_args
    assert 'SELECT * FROM my_fake_entity' in args[0]
    assert 'WHERE id=:id AND title=:title AND content=:content' in args[0]

def test_repository_get_or_create_generate_correct_create_sql(mocker):
    PARAMS_DICT = {'id':1, 'title': 'essai', 'content': 'lorem ipsum'}
    class MyFakeEntity:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
    Database = mocker.patch('records.Database')
    Database.return_value.query.return_value.all.side_effect = [
        [], [PARAMS_DICT], [PARAMS_DICT], [PARAMS_DICT], [PARAMS_DICT]
    ]
    repository = Repository(MyFakeEntity)
    instance = repository.get_or_create(**PARAMS_DICT)
    args, kwargs = Database().query.call_args_list[1]
    assert 'INSERT INTO my_fake_entity(id, title, content)' in args[0]
    assert 'VALUES (:id, :title, :content)' in args[0]

def test_repository_get_or_create_generate_correct_create_sql(mocker):
    PARAMS_DICT = {'id':1, 'title': 'essai', 'content': 'lorem ipsum'}
    class MyFakeEntity:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
    Database = mocker.patch('records.Database')
    Database.return_value.query.return_value.all.side_effect = [
        [], [PARAMS_DICT], [PARAMS_DICT], [PARAMS_DICT], [PARAMS_DICT]
    ]
    repository = Repository(MyFakeEntity)
    instance = repository.get_or_create(**PARAMS_DICT)
    args, kwargs = Database.return_value.query.call_args_list[1]
    assert 'INSERT INTO my_fake_entity(id, title, content)' in args[0]
    assert 'VALUES (:id, :title, :content)' in args[0]

def test_repository_get_or_create_generate_correct_getlast_sql(mocker):
    PARAMS_DICT = {'id':1, 'title': 'essai', 'content': 'lorem ipsum'}
    class MyFakeEntity:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
    Database = mocker.patch('records.Database')
    Database.return_value.query.return_value.all.side_effect = [
        [], [PARAMS_DICT], [PARAMS_DICT], [PARAMS_DICT], [PARAMS_DICT]
    ]
    repository = Repository(MyFakeEntity)
    instance = repository.get_or_create(**PARAMS_DICT)
    args, kwargs = Database.return_value.query.call_args_list[3]
    assert 'SELECT * FROM my_fake_entity WHERE id=:id' in args[0]

def test_repository_get_or_create_generate_does_not_create_if_found(mocker):
    PARAMS_DICT = {'id':1, 'title': 'essai', 'content': 'lorem ipsum'}
    class MyFakeEntity:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
    Database = mocker.patch('records.Database')
    Database.return_value.query.return_value.all.return_value = [PARAMS_DICT]
    repository = Repository(MyFakeEntity)
    instance = repository.get_or_create(**PARAMS_DICT)
    assert len(Database.return_value.query.call_args_list) == 1

def test_repository_get_or_create_returns_instance_if_found(mocker):
    PARAMS_DICT = {'id':1, 'title': 'essai', 'content': 'lorem ipsum'}
    class MyFakeEntity:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
    Database = mocker.patch('records.Database')
    Database.return_value.query.return_value.all.return_value = [PARAMS_DICT]
    repository = Repository(MyFakeEntity)
    instance = repository.get_or_create(**PARAMS_DICT)
    assert isinstance(instance, MyFakeEntity)

def test_repository_get_or_create_returns_instance_if_created(mocker):
    PARAMS_DICT = {'id':1, 'title': 'essai', 'content': 'lorem ipsum'}
    class MyFakeEntity:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
    Database = mocker.patch('records.Database')
    Database.return_value.query.return_value.all.side_effect = [
        [], [PARAMS_DICT], [PARAMS_DICT], [PARAMS_DICT], [PARAMS_DICT]
    ]
    repository = Repository(MyFakeEntity)
    instance = repository.get_or_create(**PARAMS_DICT)
    assert isinstance(instance, MyFakeEntity)

def test_repository_filter_returns_instances_if_found(mocker):
    ROWS = [
        {'id':1, 'title': 'essai', 'content': 'lorem ipsum'},
        {'id':2, 'title': 'essai 2', 'content': 'lorem ipsum 2'}
    ]
    class MyFakeEntity:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
    Database = mocker.patch('records.Database')
    Database.return_value.query.return_value.all.return_value = ROWS
    repository = Repository(MyFakeEntity)
    instances = repository.filter(title='essai', content='lorem')
    assert isinstance(instances[0], MyFakeEntity)
    assert isinstance(instances[1], MyFakeEntity)

def test_repository_filter_returns_empty_list_if_nothing_found(mocker):
    ROWS = []
    class MyFakeEntity:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
    Database = mocker.patch('records.Database')
    Database.return_value.query.return_value.all.return_value = ROWS
    repository = Repository(MyFakeEntity)
    instances = repository.filter(title='essai', content='lorem')
    assert instances == []

def test_repository_get_returns_instance_if_found(mocker):
    ROWS = [
        {'id':1, 'title': 'essai', 'content': 'lorem ipsum'},
        {'id':2, 'title': 'essai 2', 'content': 'lorem ipsum 2'}
    ]
    class MyFakeEntity:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
    Database = mocker.patch('records.Database')
    Database.return_value.query.return_value.all.return_value = ROWS
    repository = Repository(MyFakeEntity)
    instance = repository.get(title='essai', content='lorem')
    assert isinstance(instance, MyFakeEntity)

def test_repository_get_returns_none_if_nothing_found(mocker):
    ROWS = []
    class MyFakeEntity:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
    Database = mocker.patch('records.Database')
    Database.return_value.query.return_value.all.return_value = ROWS
    repository = Repository(MyFakeEntity)
    instance = repository.get(title='essai', content='lorem')
    assert instance is None

def test_repository_save_generate_correct_create_sql(mocker):
    @dataclass
    class MyFakeEntity:
        title: str
        content: str
        id: int = None
    Database = mocker.patch('records.Database')
    repository = Repository(MyFakeEntity)
    repository.save(MyFakeEntity(title='essai', content='lorem'))
    args, kwargs = Database.return_value.query.call_args_list[0]
    assert 'INSERT INTO my_fake_entity(title, content)' in args[0]
    assert 'VALUES (:title, :content)' in args[0]
    assert kwargs['title'] == 'essai'
    assert kwargs['content'] == 'lorem'
    assert 'id' not in kwargs

def test_repository_get_all_returns_instances_if_found(mocker):
    ROWS = [
        {'id':1, 'title': 'essai', 'content': 'lorem ipsum'},
        {'id':2, 'title': 'essai 2', 'content': 'lorem ipsum 2'}
    ]
    class MyFakeEntity:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
    Database = mocker.patch('records.Database')
    Database.return_value.query.return_value.all.return_value = ROWS
    repository = Repository(MyFakeEntity)
    instances = repository.get_all()
    assert isinstance(instances[0], MyFakeEntity)
    assert isinstance(instances[1], MyFakeEntity)

def test_repository_get_all_returns_empty_list_if_nothing_found(mocker):
    ROWS = []
    class MyFakeEntity:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
    Database = mocker.patch('records.Database')
    Database.return_value.query.return_value.all.return_value = ROWS
    repository = Repository(MyFakeEntity)
    instances = repository.get_all()
    assert instances == []

def test_model_decorator_transforms_to_dataclass(mocker):
    mocker.patch('records.Database')
    @model
    class MyFakeEntity:
        title: str
        content: str
        id: int = None
    instance = MyFakeEntity(title='title', content='content', id=1)
    assert instance.id == 1
    assert instance.title == 'title'
    assert instance.content == 'content'
    assert isinstance(MyFakeEntity.objects, Repository)
    assert MyFakeEntity.objects.model == MyFakeEntity

def test_model_decorator_save_calls_repository_save(mocker):
    mocker.patch('records.Database')
    mocker.patch('zentity.core.Repository')
    @model
    class MyFakeEntity:
        title: str
        content: str
        id: int = None
    instance = MyFakeEntity(title='title', content='content', id=1)
    instance.save()
    args, kwargs = instance.objects.save.call_args
    assert args[0] == instance

def test_repository_get_or_save(mocker):
    Database = mocker.patch('records.Database')
    _create = mocker.patch('zentity.core.Repository._create')
    Database().query().all.return_value = [
        {'modified': 1, 'not_modified':None, 'a': 'A', 'b': 'B'}
    ]
    @dataclass
    class MyFakeEntity:
        a: str
        b: str
        modified: int = None
        not_modified: str = None
    repo = Repository(MyFakeEntity)
    instance = MyFakeEntity(a='A', b='B')
    inst = repo.get_or_save(instance)
    assert instance.modified == 1
    assert instance.not_modified is None
    assert instance.a == 'A'
    assert instance.b == 'B'
    assert not _create.called
    assert inst == instance

