unit _bson;
{$IFDEF FPC}
{$MODE DELPHI}
{$ENDIF}
interface

uses
  SysUtils,
  Classes,
  Contnrs;
{
  BSON element format
  <type:byte> <c-str> <data>
  <data> below
}

const
  BSON_EOF          = $00;
  BSON_FLOAT        = $01; //double 8-byte float
  BSON_STRING       = $02; //UTF-8 string
  BSON_DOC          = $03; //embedded document
  BSON_ARRAY        = $04; //bson document but using integer string for key
  BSON_BINARY       = $05; //
  BSON_UNDEFINED    = $06; //deprecated
  BSON_OBJECTID     = $07; //
  BSON_BOOLEAN      = $08; //false:$00, true:$01
  BSON_DATETIME     = $09;
  BSON_NULL         = $0A;
  BSON_REGEX        = $0B; //
  BSON_DBPTR        = $0C; //deprecated
  BSON_JS           = $0D;
  BSON_SYMBOL       = $0E;
  BSON_JSSCOPE      = $0F;
  BSON_INT32        = $10;
  BSON_TIMESTAMP    = $11;
  BSON_INT64        = $12;
  BSON_MINKEY       = $FF;
  BSON_MAXKEY       = $7F;
  {subtype}
  BSON_SUBTYPE_FUNC = $01;
  BSON_SUBTYPE_BINARY = $02;
  BSON_SUBTYPE_UUID = $03;
  BSON_SUBTYPE_MD5  = $05;
  BSON_SUBTYPE_USER = $80;
  {boolean constant}
  BSON_BOOL_FALSE   = $00;
  BSON_BOOL_TRUE    = $01;

type
  EBSONException = class( Exception );
  TBSONObjectID = array[0..11] of byte;
  TBSONDocument = class;
  TBSONItem = class
  protected
    eltype: byte;
    elname: string;
    fnull: boolean;

    procedure WriteDouble( Value: real ); virtual;
    procedure WriteInteger( Value: integer ); virtual;
    procedure WriteInt64( Value: Int64 ); virtual;
    procedure WriteBoolean( Value: Boolean ); virtual;
    procedure WriteString( Value: string ); virtual;
    procedure WriteOID( Value: TBSONObjectID ); virtual;
    procedure WriteDocument( Value: TBSONDocument ); virtual;
    procedure WriteItem( idx: integer; Value: TBSONItem ); virtual;

    function ReadDouble: real; virtual;
    function ReadInteger: integer; virtual;
    function ReadInt64: Int64; virtual;
    function ReadBoolean: Boolean; virtual;
    function ReadString: string; virtual;
    function ReadOID: TBSONObjectID; virtual;
    function ReadDocument: TBSONDocument; virtual;
    function ReadItem( idx: integer ): TBSONItem; virtual;
  public
    constructor Create( etype: byte = BSON_NULL );

    procedure WriteStream( F: TStream ); virtual;
    procedure ReadStream( F: TStream ); virtual;

    function GetSize: longint; virtual;

    function IsNull: boolean;
    property AsInteger: integer read ReadInteger write WriteInteger;
    property AsDouble: real read ReadDouble write WriteDouble;
    property AsInt64: int64 read ReadInt64 write WriteInt64;
    property AsString: string read ReadString write WriteString;
    property AsBoolean: Boolean read ReadBoolean write WriteBoolean;
    property Items[idx: integer]: TBSONItem read ReadItem write WriteItem;
    property Name: string read elname;
  end;

  TBSONDocument = class
    FItems: array of TBSONItem;
    function GetItem( i: integer ): TBSONItem;
    function GetValue( name: string ): TBSONItem;
    procedure SetValue( Name: string; Value: TBSONItem );
    function GetCount: integer;
  public
    constructor Create;
    destructor Free;

    procedure Clear;
    procedure ReadStream( F: TStream );
    procedure WriteStream( F: TStream );

    procedure LoadFromFile( filename: string );
    procedure SaveToFile( filename: string );

    function IndexOf( name: string ): integer;
    function GetSize: longint;

    property Items[idx: integer]: TBSONItem read GetItem;
    property Values[Name: string]: TBSONItem read GetValue write SetValue;
    property Count: integer read GetCount;
  end;

  TBSONDoubleItem = class( TBSONItem )
    FData: real;

    procedure WriteDouble( AValue: real ); override;
    function ReadDouble: real; override;
  public
    constructor Create( AValue: real = 0.0 );
    function GetSize: longint; override;

    procedure ReadStream( F: TStream ); override;
    procedure WriteStream( F: TStream ); override;
  end;

  TBSONIntItem = class( TBSONItem )
    FData: integer;

    procedure WriteInteger( AValue: integer ); override;
    function ReadInteger: integer; override;
  public
    constructor Create( AValue: integer = 0 );
    function GetSize: longint; override;

    procedure ReadStream( F: TStream ); override;
    procedure WriteStream( F: TStream ); override;
  end;

  TBSONStringItem = class( TBSONItem )
  protected
    FData: string;

    procedure WriteString( AValue: string ); override;
    function ReadString: string; override;
  public
    constructor Create( AValue: string = '' );
    function GetSize: longint; override;

    procedure ReadStream( F: TStream ); override;
    procedure WriteStream( F: TStream ); override;
  end;

  TBSONJSItem = class( TBSONStringItem )
  public
    constructor Create( AValue: string = '' );
  end;

  TBSONSymbolItem = class( TBSONStringItem )
  public
    constructor Create( AValue: string = '' );
  end;

  TBSONInt64Item = class( TBSONItem )
    FData: Int64;

    procedure WriteInt64( AValue: int64 ); override;
    function ReadInt64: Int64; override;
  public
    constructor Create( AValue: Int64 = 0 );
    function GetSize: longint; override;

    procedure ReadStream( F: TStream ); override;
    procedure WriteStream( F: TStream ); override;
  end;

  TBSONBooleanItem = class( TBSONItem )
    FData: Boolean;

    procedure WriteBoolean( AValue: Boolean ); override;
    function ReadBoolean: Boolean; override;
  public
    constructor Create( AValue: Boolean = false );
    function GetSize: longint; override;

    procedure ReadStream( F: TStream ); override;
    procedure WriteStream( F: TStream ); override;
  end;

  TBSONDocumentItem = class( TBSONItem )
    FData: TBSONDocument;
  public
    constructor Create;
    destructor Free;
    function GetSize: longint; override;

    procedure ReadStream( F: TStream ); override;
    procedure WriteStream( F: TStream ); override;
  end;

  TBSONArrayItem = class( TBSONItem )
    FData: TBSONDocument;

    procedure WriteItem( idx: integer; item: TBSONItem ); override;
    function ReadItem( idx: integer ): TBSONItem; override;
  public
    constructor Create;
    destructor Free;
    function GetSize: longint; override;

    procedure ReadStream( F: TStream ); override;
    procedure WriteStream( F: TStream ); override;
  end;

  TBSONDatetimeItem = class( TBSONItem )
    FData: TDatetime;
  public
    constructor Create( AValue: TDateTime );
    function GetSize: longint; override;

    procedure ReadStream( F: TStream ); override;
    procedure WriteStream( F: TStream ); override;
  end;

  TBSONBinaryItem = class( TBSONItem )
    FLen: integer;
    FSubtype: byte;
    FData: Pointer;
  public
    constructor Create;
    destructor Free;
    function GetSize: longint; override;

    procedure ReadStream( F: TStream ); override;
    procedure WriteStream( F: TStream ); override;
  end;

  TBSONObjectIDItem = class( TBSONItem )
    FData: TBSONObjectID;

    procedure WriteOID( AValue: TBSONObjectID ); override;
    function ReadOID: TBSONObjectID; override;
  public
    constructor Create( AValue: string = '000000000000' );
    function GetSize: longint; override;

    procedure ReadStream( F: TStream ); override;
    procedure WriteStream( F: TStream ); override;
  end;

  TBSONDBRefItem = class( TBSONStringItem )
    FData: array[0..11] of byte;
  public
    constructor Create( AValue: string = ''; AData: string = '' );
    function GetSize: longint; override;

    procedure ReadStream( F: TStream ); override;
    procedure WriteStream( F: TStream ); override;
  end;

  TBSONRegExItem = class( TBSONItem )
    FPattern, FOptions: string;
  public
    constructor Create( APattern: string = ''; AOptions: string = '' );
    function GetSize: longint; override;

    procedure ReadStream( F: TStream ); override;
    procedure WriteStream( F: TStream ); override;
  end;

  TBSONScopedJSItem = class( TBSONItem )
    FLen: integer;
    FCode: string;
    FScope: TBSONDocument;
  public
    constructor Create;
    destructor Free;
    function GetSize: longint; override;

    procedure ReadStream( F: TStream ); override;
    procedure WriteStream( F: TStream ); override;
  end;

function _ReadString( F: TStream ): string;

implementation

uses
  DateUtils;

const
  nullterm          : char = #0;

var
  buf               : array[0..65535] of char;
  nullitem          : TBSONItem;

function _ReadString( F: TStream ): string;
var
  i                 : integer;
  c                 : char;
begin
  i := 0;
  repeat
    f.read( c, sizeof( char ) );
    buf[i] := c;
    inc( i );
  until c = nullterm;
  result := strpas( buf );
end;

{ TBSONDocument }

procedure TBSONDocument.Clear;
var
  i                 : integer;
begin
  for i := 0 to High( Fitems ) do begin
    FItems[i].Free;
  end;
  SetLength( FItems, 0 );
end;

constructor TBSONDocument.Create;
begin
  SetLength( FItems, 0 );
end;

destructor TBSONDocument.Free;
begin
  Clear;
end;

function TBSONDocument.GetCount: integer;
begin
  Result := Length( FItems );
end;

function TBSONDocument.GetItem( i: integer ): TBSONItem;
begin
  if i in [0..high( FItems )] then
    Result := FItems[i]
  else
    Result := nullitem;
end;

function TBSONDocument.GetSize: longint;
var
  i                 : integer;
begin
  Result := 5;
  for i := 0 to high( FItems ) do begin
    Result := Result + FItems[i].GetSize;
  end;
end;

function TBSONDocument.GetValue( name: string ): TBSONItem;
var
  i                 : integer;
begin
  result := nullitem;
  i := IndexOf( name );
  if i <> -1 then Result := FItems[i];
end;

function TBSONDocument.IndexOf( name: string ): integer;
var
  i                 : integer;
begin
  Result := -1;
  for i := 0 to high( FItems ) do begin
    if FItems[i].elname = name then begin
      Result := i;
      break;
    end;
  end;
end;

procedure TBSONDocument.LoadFromFile( filename: string );
var
  f                 : TFileStream;
begin
  f := TFileStream.Create( filename, fmOpenRead );
  ReadStream( f );
  f.Free;
end;

procedure TBSONDocument.ReadStream( F: TStream );
var
  len               : integer;
  elmtype           : byte;
  elmname           : string;
begin
  Clear;
  f.Read( len, sizeof( len ) );
  f.Read( elmtype, sizeof( byte ) );
  while elmtype <> BSON_EOF do begin
    elmname := _ReadString( f );
    SetLength( FItems, length( FItems ) + 1 );
    case elmtype of
      BSON_ARRAY: FItems[high( FItems )] := TBSONArrayItem.Create;
      BSON_BINARY: FItems[high( FItems )] := TBSONBinaryItem.Create;
      BSON_DBPTR: FItems[high( FItems )] := TBSONDBRefItem.Create;
      BSON_FLOAT: FItems[high( FItems )] := TBSONDoubleItem.Create;
      BSON_INT32: FItems[high( FItems )] := TBSONIntItem.Create;
      BSON_INT64: FItems[high( FItems )] := TBSONInt64Item.Create;
      BSON_BOOLEAN: FItems[high( FItems )] := TBSONBooleanItem.Create;
      BSON_STRING: FItems[high( FItems )] := TBSONStringItem.Create;
      BSON_DOC: FItems[high( FItems )] := TBSONDocumentItem.Create;
      BSON_JS: FItems[high( FItems )] := TBSONJSItem.Create;
      BSON_JSSCOPE: FItems[high( FItems )] := TBSONScopedJSItem.Create;
      BSON_OBJECTID: FItems[high( FItems )] := TBSONObjectIDItem.Create;
      BSON_MINKEY: FItems[high( FItems )] := TBSONItem.Create( BSON_MINKEY );
      BSON_MAXKEY: FItems[high( FItems )] := TBSONItem.Create( BSON_MAXKEY );
      BSON_REGEX: FItems[high( FItems )] := TBSONRegExItem.Create;
      BSON_SYMBOL: FItems[high( FItems )] := TBSONSymbolItem.Create;
    else
      raise EBSONException.Create( 'unimplemented element handler ' + inttostr( elmtype ) );
    end;
    with FItems[high( FItems )] do begin
      elname := elmname;
      ReadStream( f );
    end;
    f.Read( elmtype, sizeof( byte ) );
  end;
end;

procedure TBSONDocument.SaveToFile( filename: string );
var
  f                 : TFileStream;
begin
{$IFDEF FPC}
  f := TFileStream.Create( filename, fmOpenWrite );
{$ELSE}
  f := TFileStream.Create( FileCreate( filename ) );
{$ENDIF}
  WriteStream( f );
  f.Free;
end;

procedure TBSONDocument.SetValue( Name: string; Value: TBSONItem );
var
  item              : TBSONItem;
  idx               : integer;
begin
  idx := IndexOf( name );
  if idx = -1 then begin
    Value.elname := Name;
    SetLength( FItems, Length( FItems ) + 1 );
    FItems[high( FItems )] := Value;
  end
  else begin
    item := FItems[idx];
    if ( item.eltype <> value.eltype ) then begin
      FItems[idx] := Value;
      item.Free;
    end;
  end;
end;

procedure TBSONDocument.WriteStream( F: TStream );
var
  dummy             : integer;
  i                 : integer;
begin
  dummy := GetSize;
  f.write( dummy, sizeof( dummy ) );
  for i := 0 to high( FItems ) do begin
    FItems[i].WriteStream( f );
  end;
  f.Write( nullterm, sizeof( nullterm ) );
end;

{ TBSONDoubleItem }

constructor TBSONDoubleItem.Create( AValue: real );
begin
  eltype := BSON_FLOAT;
  FData := AValue;
end;

function TBSONDoubleItem.GetSize: longint;
begin
  Result := 2 + length( elname ) + sizeof( FData );
end;

function TBSONDoubleItem.ReadDouble: real;
begin
  Result := FData;
end;

procedure TBSONDoubleItem.ReadStream( F: TStream );
begin
  f.Read( FData, sizeof( FData ) );
end;

procedure TBSONDoubleItem.WriteDouble( AValue: real );
begin
  FData := AValue;
end;

procedure TBSONDoubleItem.WriteStream( F: TStream );
begin
  f.Write( eltype, sizeof( byte ) );
  f.Write( elname[1], length( elname ) );
  f.Write( nullterm, sizeof( nullterm ) );
  f.Write( FData, sizeof( FData ) );
end;

{ TBSONIntItem }

constructor TBSONIntItem.Create( AValue: integer );
begin
  eltype := BSON_INT32;
  FData := AValue;
end;

function TBSONIntItem.GetSize: longint;
begin
  Result := 2 + length( elname ) + sizeof( FData );
end;

function TBSONIntItem.ReadInteger: integer;
begin
  result := FData;
end;

procedure TBSONIntItem.ReadStream( F: TStream );
begin
  f.Read( fdata, sizeof( integer ) );
end;

procedure TBSONIntItem.WriteInteger( AValue: integer );
begin
  FData := AValue;
end;

procedure TBSONIntItem.WriteStream( F: TStream );
begin
  f.Write( eltype, sizeof( byte ) );
  f.Write( elname[1], length( elname ) );
  f.Write( nullterm, sizeof( nullterm ) );
  f.Write( FData, sizeof( FData ) );
end;

{ TBSONStringItem }

constructor TBSONStringItem.Create( AValue: string );
begin
  eltype := BSON_STRING;
  FData := AValue;
end;

function TBSONStringItem.GetSize: longint;
begin
  Result := 7 + length( elname ) + length( fdata );
end;

procedure TBSONStringItem.ReadStream( F: TStream );
var
  len               : integer;
begin
  f.Read( len, sizeof( integer ) );
  FData := _ReadString( F );
end;

function TBSONStringItem.ReadString: string;
begin
  Result := FData;
end;

procedure TBSONStringItem.WriteStream( F: TStream );
var
  len               : integer;
begin
  len := length( FData ) + 1;
  f.Write( eltype, sizeof( byte ) );
  f.Write( elname[1], length( elname ) );
  f.Write( nullterm, sizeof( nullterm ) );
  f.Write( len, sizeof( integer ) );
  f.Write( FData[1], length( FData ) );
  f.Write( nullterm, sizeof( nullterm ) );
end;

procedure TBSONStringItem.WriteString( AValue: string );
begin
  FData := AValue;
end;

{ TBSONInt64Item }

constructor TBSONInt64Item.Create( AValue: Int64 );
begin
  eltype := BSON_INT64;
  FData := AValue;
end;

function TBSONInt64Item.GetSize: longint;
begin
  Result := 2 + length( elname ) + sizeof( fdata );
end;

function TBSONInt64Item.ReadInt64: Int64;
begin
  Result := FData;
end;

procedure TBSONInt64Item.ReadStream( F: TStream );
begin
  f.Read( FData, sizeof( FData ) );
end;

procedure TBSONInt64Item.WriteInt64( AValue: int64 );
begin
  FData := AValue;
end;

procedure TBSONInt64Item.WriteStream( F: TStream );
begin
  f.Write( eltype, sizeof( byte ) );
  f.Write( elname[1], length( elname ) );
  f.Write( nullterm, sizeof( nullterm ) );
  f.Write( FData, sizeof( FData ) );
end;

{ TBSONBooleanItem }

constructor TBSONBooleanItem.Create( AValue: Boolean );
begin
  eltype := BSON_BOOLEAN;
  FData := AValue;
end;

function TBSONBooleanItem.GetSize: longint;
begin
  Result := 3 + length( elname );
end;

function TBSONBooleanItem.ReadBoolean: Boolean;
begin
  Result := FData;
end;

procedure TBSONBooleanItem.ReadStream( F: TStream );
var
  b                 : Byte;
begin
  f.Read( b, sizeof( byte ) );
  FData := b = BSON_BOOL_TRUE
end;

procedure TBSONBooleanItem.WriteBoolean( AValue: Boolean );
begin
  FData := AValue;
end;

procedure TBSONBooleanItem.WriteStream( F: TStream );
var
  boolb             : byte;
begin
  f.Write( eltype, sizeof( byte ) );
  f.Write( elname[1], length( elname ) );
  f.Write( nullterm, sizeof( nullterm ) );
  if FData then
    boolb := BSON_BOOL_TRUE
  else
    boolb := BSON_BOOL_FALSE;
  f.Write( boolb, sizeof( byte ) );
end;

{ TBSONItem }

constructor TBSONItem.Create( etype: byte );
begin
  fnull := true;
  eltype := etype;
end;

function TBSONItem.GetSize: longint;
begin
  Result := 0;
  if FNull then
    Result := 2 + Length( elName );
end;

function TBSONItem.IsNull: boolean;
begin
  Result := FNull;
end;

function TBSONItem.ReadBoolean: Boolean;
begin
  Result := False;
end;

function TBSONItem.ReadDocument: TBSONDocument;
begin
  Result := nil;
end;

function TBSONItem.ReadDouble: real;
begin
  Result := 0;
end;

function TBSONItem.ReadInt64: Int64;
begin
  Result := 0;
end;

function TBSONItem.ReadInteger: integer;
begin
  Result := 0;
end;

function TBSONItem.ReadItem( idx: integer ): TBSONItem;
begin
  Result := nullitem;
end;

function TBSONItem.ReadOID: TBSONObjectID;
begin
  Result := Result;
end;

procedure TBSONItem.ReadStream( F: TStream );
begin

end;

function TBSONItem.ReadString: string;
begin
  Result := '';
end;

procedure TBSONItem.WriteBoolean( Value: Boolean );
begin

end;

procedure TBSONItem.WriteDocument( Value: TBSONDocument );
begin

end;

procedure TBSONItem.WriteDouble( Value: real );
begin

end;

procedure TBSONItem.WriteInt64( Value: Int64 );
begin

end;

procedure TBSONItem.WriteInteger( Value: integer );
begin

end;

procedure TBSONItem.WriteItem( idx: integer; Value: TBSONItem );
begin

end;

procedure TBSONItem.WriteOID( Value: TBSONObjectID );
begin

end;

procedure TBSONItem.WriteStream( F: TStream );
begin
  if FNull then begin
    f.Write( eltype, sizeof( byte ) );
    f.Write( elname[1], length( elname ) );
    f.Write( nullterm, sizeof( nullterm ) );
  end;
end;

procedure TBSONItem.WriteString( Value: string );
begin

end;

{ TBSONDocumentItem }

constructor TBSONDocumentItem.Create;
begin
  FData := TBSONDocument.Create;
end;

destructor TBSONDocumentItem.Free;
begin
  FData.Free;
end;

function TBSONDocumentItem.GetSize: longint;
begin
  Result := 2 + length( elname ) + FData.GetSize;
end;

procedure TBSONDocumentItem.ReadStream( F: TStream );
begin
  inherited;
  FData.ReadStream( f );
end;

procedure TBSONDocumentItem.WriteStream( F: TStream );
begin
  f.Write( eltype, sizeof( byte ) );
  f.Write( elname[1], length( elname ) );
  f.Write( nullterm, sizeof( nullterm ) );
  FData.WriteStream( f );
end;

{ TBSONArrayItem }

constructor TBSONArrayItem.Create;
begin
  eltype := BSON_ARRAY;
  FData := TBSONDocument.Create;
end;

destructor TBSONArrayItem.Free;
begin
  FData.Free;
end;

function TBSONArrayItem.GetSize: longint;
begin
  Result := 2 + length( elname ) + FData.GetSize;
end;

function TBSONArrayItem.ReadItem( idx: integer ): TBSONItem;
begin
  Result := FData.Items[idx];
end;

procedure TBSONArrayItem.ReadStream( F: TStream );
begin
  FData.ReadStream( F );
end;

procedure TBSONArrayItem.WriteItem( idx: integer; item: TBSONItem );
begin
  inherited;
  FData.SetValue( IntToStr( idx ), item );
end;

procedure TBSONArrayItem.WriteStream( F: TStream );
begin
  f.Write( eltype, sizeof( byte ) );
  f.Write( elname[1], length( elname ) );
  f.Write( nullterm, sizeof( nullterm ) );
  FData.WriteStream( f );
end;

{ TBSONDatetimeItem }

constructor TBSONDatetimeItem.Create( AValue: TDateTime );
begin
  eltype := BSON_DATETIME;
  FData := AValue;
end;

function TBSONDatetimeItem.GetSize: longint;
begin
  result := 2 + length( elname ) + sizeof( int64 );
end;

procedure TBSONDatetimeItem.ReadStream( F: TStream );
var
  data              : int64;
begin
  f.Read( data, sizeof( int64 ) );
  FData := UnixToDateTime( data );
end;

procedure TBSONDatetimeItem.WriteStream( F: TStream );
var
  data              : Int64;
begin
  data := DateTimeToUnix( FData );
  f.Write( eltype, sizeof( byte ) );
  f.Write( elname[1], length( elname ) );
  f.Write( nullterm, sizeof( nullterm ) );
  f.Write( Data, sizeof( int64 ) );
end;

{ TBSONJSItem }

constructor TBSONJSItem.Create( AValue: string );
begin
  inherited Create( AValue );
  eltype := BSON_JS;
end;

{ TBSONObjectIDItem }

constructor TBSONObjectIDItem.Create( AValue: string );
var
  i                 : integer;
begin
  eltype := BSON_OBJECTID;
  if length( AValue ) = 12 then
    for i := 0 to 11 do
      FData[i] := StrToInt( AValue[i + 1] );
end;

function TBSONObjectIDItem.GetSize: longint;
begin
  result := 2 + length( elname ) + 12;
end;

function TBSONObjectIDItem.ReadOID: TBSONObjectID;
begin
  Result := FData;
end;

procedure TBSONObjectIDItem.ReadStream( F: TStream );
begin
  f.Read( FData[0], 12 );
end;

procedure TBSONObjectIDItem.WriteOID( AValue: TBSONObjectID );
begin
  FData := AValue;
end;

procedure TBSONObjectIDItem.WriteStream( F: TStream );
begin
  f.Write( eltype, sizeof( byte ) );
  f.Write( elname[1], length( elname ) );
  f.Write( nullterm, sizeof( nullterm ) );
  f.Write( FData[0], 12 );
end;

{ TBSONRegExItem }

constructor TBSONRegExItem.Create( APattern, AOptions: string );
begin
  FPattern := APattern;
  FOptions := AOptions;
  eltype := BSON_REGEX;
end;

function TBSONRegExItem.GetSize: longint;
begin
  result := 2 + length( elname ) + 1 + length( FPattern ) + 1 + length( FOptions );
end;

procedure TBSONRegExItem.ReadStream( F: TStream );
begin
  FPattern := _ReadString( f );
  FOptions := _ReadString( f );
end;

procedure TBSONRegExItem.WriteStream( F: TStream );
begin
  f.Write( eltype, sizeof( byte ) );
  f.Write( elname[1], length( elname ) );
  f.Write( nullterm, sizeof( nullterm ) );
  f.Write( FPattern[1], length( FPattern ) );
  f.Write( nullterm, sizeof( nullterm ) );
  f.Write( FOptions[1], length( FOptions ) );
  f.Write( nullterm, sizeof( nullterm ) );
end;

{ TBSONBinaryItem }

constructor TBSONBinaryItem.Create;
begin
  FLen := 0;
  FData := nil;
  FSubtype := BSON_SUBTYPE_USER;
  eltype := BSON_BINARY;
end;

destructor TBSONBinaryItem.Free;
begin
  if FLen <> 0 then begin
    FreeMem( FData );
  end;
end;

function TBSONBinaryItem.GetSize: longint;
begin
  result := 2 + length( elname ) + 4 + 1 + FLen;
end;

procedure TBSONBinaryItem.ReadStream( F: TStream );
begin
  f.Read( FLen, sizeof( integer ) );
  f.Read( FSubtype, sizeof( byte ) );
  GetMem( FData, FLen );
  f.Read( FData, Flen );
end;

procedure TBSONBinaryItem.WriteStream( F: TStream );
begin
  f.Write( eltype, sizeof( byte ) );
  f.Write( elname[1], length( elname ) );
  f.Write( nullterm, sizeof( nullterm ) );
  f.Write( FLen, sizeof( integer ) );
  f.Write( FSubtype, sizeof( byte ) );
  f.Write( FData, FLen );
end;

{ TBSONScopedJSItem }

constructor TBSONScopedJSItem.Create;
begin
  eltype := BSON_JSSCOPE;
  FScope := TBSONDocument.Create;
end;

destructor TBSONScopedJSItem.Free;
begin
  FScope.Free;
end;

function TBSONScopedJSItem.GetSize: longint;
begin
  result := 2 + length( elname ) + 4 + length( fcode ) + 1 + FScope.GetSize;
end;

procedure TBSONScopedJSItem.ReadStream( F: TStream );
begin
  f.Read( Flen, sizeof( integer ) );
  FCode := _ReadString( f );
  FScope.ReadStream( f );
end;

procedure TBSONScopedJSItem.WriteStream( F: TStream );
begin
  f.Write( eltype, sizeof( byte ) );
  f.Write( elname[1], length( elname ) );
  f.Write( nullterm, sizeof( nullterm ) );
  FLen := FScope.GetSize + 5 + length( FCode );
  f.Write( FLen, sizeof( integer ) );
  f.Write( FCode[1], length( FCode ) );
  f.Write( nullterm, sizeof( nullterm ) );
  FScope.WriteStream( f );
end;

{ TBSONSymbolItem }

constructor TBSONSymbolItem.Create( AValue: string );
begin
  eltype := BSON_SYMBOL;
end;

{ TBSONDBRefItem }

constructor TBSONDBRefItem.Create( AValue, AData: string );
var
  i                 : integer;
begin
  inherited Create( AValue );
  eltype := BSON_DBPTR;
  if length( AData ) = 12 then
    for i := 0 to 11 do
      FData[i] := StrToInt( AData[1 + i] );
end;

function TBSONDBRefItem.GetSize: longint;
begin
  result := 2 + length( elname ) + 12;
end;

procedure TBSONDBRefItem.ReadStream( F: TStream );
begin
  inherited;
  f.Read( FData[0], 12 );
end;

procedure TBSONDBRefItem.WriteStream( F: TStream );
begin
  inherited;
  f.Write( FData[0], 12 );
end;

initialization
  nullitem := TBSONItem.Create;
finalization
  nullitem.Free;
end.

