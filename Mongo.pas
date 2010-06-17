unit Mongo;
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
    constructor Create( host: string = 'localhost'; port: string = '27017' );
    destructor Free;

    function GetCollection( collName: string ): TMongoCollection;
    procedure SelectDatabase( db: string );

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

procedure mongo_query( conn: TMongoConnection; db, collection: string; nToSkip, nToReturn: integer; query: TBSONDocument = nil; fields: TBSONDocument = nil; options: integer = 0 );

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

{ TMongoConnection }

constructor TMongoConnection.Create( host, port: string );
begin
  FHost := host;
  FPort := port;
  FSocket := TTCPBlockSocket.Create;
  try
    FSocket.Connect( FSocket.ResolveName( host ), port );
    FConnected := true;
    FSocket.SetTimeout( _timeout );
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

procedure TMongoConnection.SelectDatabase( db: string );
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

