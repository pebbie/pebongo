unit MongoDB;
{$IFDEF FPC}
{$MODE DELPHI}
{$ENDIF}

interface
uses
  _bson,
  SysUtils,
  blcksock
  ;

type
  EMongoException = class( Exception );
  TMongoCollection = class;
  TMongoConnection = class
  protected
    FHost: string;
    FPort: string;
    FSocket: TTCPBlockSocket;
    FConnected: Boolean;
    FDB: string;
  public
    constructor Create( host: string = 'localhost'; port: string = '27017'; db: string = '' );
    destructor Free;

    function GetCollection( collName: string ): TMongoCollection;
    procedure GetDatabase( db: string );

    property Socket: TTCPBlockSocket read FSocket;
    property Connected: Boolean read FConnected;
  end;

  TMongoCursor = class
  protected
    FCursorID: int64;
    FStart: integer;
    FCount: integer;
    FDocs: array of TBSONDocument;
    FConnection: TMongoConnection;
    function GetDoc( idx: integer ): TBSONDocument;
  public
    constructor Create( conn: TMongoConnection );
    property Count: integer read FCount;
    property Result[idx: integer]: TBSONDocument read GetDoc;
  end;

  TMongoCollection = class
  protected
    FDatabase: string;
    FCollection: string;
    FDoc: TBSONDocument;
    FConnection: TMongoConnection;
  public
    constructor Create( AConn: TMongoConnection );
    function find( query: TBSONDocument = nil; fields: TBSONDocument = nil ): TMongoCursor;
    function find_one( query: TBSONDocument = nil; fields: TBSONDocument = nil ): TMongoCursor;

    procedure save( doc: TBSONDocument );
    procedure remove( sel: TBSONDocument; IsSingle: Boolean = false );

    property DBName: string read FDatabase write FDatabase;
    property Name: string read FCollection write FCollection;
  end;

type
  TMongoMsgHeader = packed record
    length: integer;
    requestID: integer;
    responseTo: integer;
    opcode: integer;
  end;

{
procedure mongo_query( conn: TMongoConnection; db, collection: string; nToSkip, nToReturn: integer; query: TBSONDocument = nil; fields: TBSONDocument = nil; options: integer = 0 );
procedure mongo_insert( conn: TMongoConnection; db, collection: string; doc: TBSONDocument );
}
implementation

uses
  Classes,
  Dialogs;

const
  OP_REPLY          = 0001;
  OP_MESSAGE        = 1000; // generic msg command followed by a string */
  OP_UPDATE         = 2001; // no reply
  OP_INSERT         = 2002; // no reply
  OP_QUERY          = 2004;
  OP_GET_MORE       = 2005;
  OP_DELETE         = 2006; //no reply
  OP_KILL_CURSORS   = 2007;

var
  _timeout          : integer = 1000;

procedure mongo_query( conn: TMongoConnection; db, collection: string; nToSkip, nToReturn: integer; query: TBSONDocument = nil; fields: TBSONDocument = nil; options: integer = 0 );
var
  hdr               : TMongoMsgHeader;
  dbfull            : string;
  hasfields         : boolean;
  q                 : TBSONDocument;
  ms                : TMemoryStream;
begin
  if not conn.Connected then exit;
  dbfull := format( '%s.%s', [db, collection] );
  hasfields := fields <> nil;
  q := query;
  if q = nil then q := TBSONDocument.Create;
  with conn.Socket do begin
    hdr.length := sizeof( hdr ) + 12 + length( dbfull ) + 1 + q.GetSize;
    hdr.requestID := 123456;
    hdr.responseTo := 0;
    hdr.opcode := OP_QUERY;
    if hasfields then hdr.length := hdr.length + fields.GetSize;
    ms := TMemoryStream.Create;
    ms.Write( hdr, sizeof( hdr ) );
    ms.Write( options, sizeof( integer ) );
    ms.Write( dbfull[1], length( dbfull ) );
    ms.Write( nullterm, sizeof( char ) );
    ms.Write( ntoSkip, sizeof( integer ) );
    ms.Write( nToReturn, sizeof( integer ) );
    q.WriteStream( ms );
    if hasfields then fields.WriteStream( ms );
    ms.Seek( 0, soFromBeginning );
    SendStreamRaw( ms );
    ms.Free;
  end;
end;

procedure mongo_insert( conn: TMongoConnection; db, collection: string; doc: TBSONDocument );
var
  hdr               : TMongoMsgHeader;
  dbfull            : string;
  ms                : TMemoryStream;
  options           : integer;
begin
  if not conn.Connected then exit;
  dbfull := format( '%s.%s', [db, collection] );
  hdr.length := sizeof( hdr ) + 5 + length( dbfull ) + doc.GetSize;
  hdr.requestID := 123456;
  hdr.opcode := OP_INSERT;
  ms := TMemoryStream.Create;
  ms.Write( hdr, sizeof( hdr ) );
  options := 0;
  ms.Write( options, sizeof( integer ) );
  ms.Write( dbfull[1], length( dbfull ) );
  ms.Write( nullterm, sizeof( char ) );
  doc.WriteStream( ms );
  ms.Seek( 0, soFromBeginning );
  conn.Socket.SendStreamRaw( ms );
  ms.Free;
end;

procedure mongo_message( conn: TMongoConnection; msg: string );
var
  hdr               : TMongoMsgHeader;
  ms                : TMemoryStream;
begin
  hdr.length := sizeof( hdr ) + 1 + length( msg );
  hdr.requestID := 123456;
  hdr.opcode := OP_MESSAGE;
  ms := TmemoryStream.Create;
  ms.Write( hdr, sizeof( hdr ) );
  ms.Write( msg[1], length( msg ) );
  ms.Write( nullterm, sizeof( char ) );
  ms.Seek( 0, soFromBeginning );
  conn.Socket.SendStreamRaw( ms );
  ms.Free;
end;

procedure mongo_delete( conn: TMongoConnection; db, collection: string; sel: TBSONDocument; IsSingle: Boolean = False );
var
  hdr               : TMongoMsgHeader;
  ms                : TMemoryStream;
  flag              : integer;
  dbfull            : string;
begin
  if not conn.Connected then exit;
  dbfull := format( '%s.%s', [db, collection] );
  hdr.length := sizeof( hdr ) + 9 + length( dbfull ) + sel.GetSize;
  ms := TMemoryStream.Create;
  hdr.requestID := 123456;
  hdr.opcode := OP_DELETE;
  ms.Write( hdr, sizeof( hdr ) );
  flag := 0;
  ms.Write( flag, sizeof( integer ) );
  ms.Write( dbfull[1], length( dbfull ) );
  ms.Write( nullterm, sizeof( char ) );
  if IsSingle then flag := 1;
  ms.Write( flag, sizeof( integer ) );
  sel.WriteStream( ms );
  ms.Seek( 0, soFromBeginning );
  conn.Socket.SendStreamRaw( ms );
  ms.Free;
end;

{ TMongoConnection }

constructor TMongoConnection.Create( host, port, db: string );
begin
  FHost := host;
  FPort := port;
  FSocket := TTCPBlockSocket.Create;
  try
    FSocket.Connect( FSocket.ResolveName( host ), port );
    FConnected := true;
    FSocket.SetTimeout( _timeout );
    if length( db ) > 0 then GetDatabase( db );
  finally
  end;
end;

destructor TMongoConnection.Free;
begin
  if FSocket.Socket <> 0 then FSocket.CloseSocket;
  FSocket.Free;
end;

function TMongoConnection.GetCollection(
  collName: string ): TMongoCollection;
begin
  if length( fdb ) = 0 then
    raise EMongoException.Create( 'Database not selected' );
  Result := TMongoCollection.Create( self );
  Result.DBName := FDB;
  Result.Name := collName;
end;

procedure TMongoConnection.GetDatabase( db: string );
begin
  self.FDB := db;
end;

{ TMongoCollection }

constructor TMongoCollection.Create( AConn: TMongoConnection );
begin
  FConnection := AConn;
end;

function TMongoCollection.find( query, fields: TBSONDocument ): TMongoCursor;
begin
  mongo_query( FConnection, FDatabase, FCollection, 0, 0, query, fields );
  Result := TMongoCursor.Create( FConnection );
end;

function TMongoCollection.find_one( query,
  fields: TBSONDocument ): TMongoCursor;
begin
  mongo_query( FConnection, FDatabase, FCollection, 0, 1, query, fields );
  Result := TMongoCursor.Create( FConnection );
end;

procedure TMongoCollection.remove( sel: TBSONDocument; IsSingle: Boolean );
begin
  mongo_delete( FConnection, FDatabase, FCollection, sel, IsSingle );
end;

procedure TMongoCollection.save( doc: TBSONDocument );
begin
  mongo_insert( FConnection, FDatabase, FCollection, doc );
end;

{ TMongoCursor }

constructor TMongoCursor.Create( conn: TMongoConnection );
var
  ms                : TMemoryStream;
  hdr               : TMongoMsgHeader;
  buf               : integer;
begin
  FConnection := conn;
  FCursorID := -1;
  FStart := 0;
  FCount := 0;
  setlength( FDocs, 0 );
  if FConnection.Connected then
    with FConnection.Socket do begin
      ms := TMemoryStream.Create;
      RecvStreamRaw( ms, _timeout );
      if ms.Size <> 0 then begin
        ms.Seek( 0, soFromBeginning );
        ms.Read( hdr, sizeof( hdr ) );
        ms.Read( buf, sizeof( buf ) );
        if buf = 0 then begin
          ms.Read( FCursorID, sizeof( int64 ) );
          ms.Read( FStart, sizeof( FStart ) );
          ms.Read( FCount, sizeof( FCount ) );
          SetLength( FDocs, FCount );
          for buf := 0 to FCount - 1 do begin
            FDocs[buf] := TBSONDocument.Create;
            FDocs[buf].ReadStream( ms );
          end;
        end;
      end;
    end;
end;

function TMongoCursor.GetDoc( idx: integer ): TBSONDocument;
begin
  Result := FDocs[idx];
end;

end.

