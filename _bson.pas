unit _bson;

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
    constructor Create;

    procedure WriteStream( F: TStream ); virtual;
    procedure ReadStream( F: TStream ); virtual;

    function IsNull: boolean;
    property AsInteger: integer read ReadInteger write WriteInteger;
    property AsDouble: real read ReadDouble write WriteDouble;
    property AsInt64: int64 read ReadInt64 write WriteInt64;
    property AsString: string read ReadString write WriteString;
    property AsBoolean: Boolean read ReadBoolean write WriteBoolean;
    property Items[idx: integer]: TBSONItem read ReadItem write WriteItem;
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

    procedure ReadStream( F: TStream ); override;
    procedure WriteStream( F: TStream ); override;
  end;

  TBSONIntItem = class( TBSONItem )
    FData: integer;

    procedure WriteInteger( AValue: integer ); override;
    function ReadInteger: integer; override;
  public
    constructor Create( AValue: integer = 0 );

    procedure ReadStream( F: TStream ); override;
    procedure WriteStream( F: TStream ); override;
  end;

  TBSONStringItem = class( TBSONItem )
    FData: string;

    procedure WriteString( AValue: string ); override;
    function ReadString: string; override;
  public
    constructor Create( AValue: string = '' );

    procedure ReadStream( F: TStream ); override;
    procedure WriteStream( F: TStream ); override;
  end;

  TBSONJSItem = class( TBSONStringItem )
  public
    constructor Create( AValue: string = '' );
  end;

  TBSONInt64Item = class( TBSONItem )
    FData: Int64;

    procedure WriteInt64( AValue: int64 ); override;
    function ReadInt64: Int64; override;
  public
    constructor Create( AValue: Int64 = 0 );

    procedure ReadStream( F: TStream ); override;
    procedure WriteStream( F: TStream ); override;
  end;

  TBSONBooleanItem = class( TBSONItem )
    FData: Boolean;

    procedure WriteBoolean( AValue: Boolean ); override;
    function ReadBoolean: Boolean; override;
  public
    constructor Create( AValue: Boolean = false );

    procedure ReadStream( F: TStream ); override;
    procedure WriteStream( F: TStream ); override;
  end;

  TBSONDocumentItem = class( TBSONItem )
    FData: TBSONDocument;
  public
    constructor Create;
    destructor Free;

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

    procedure ReadStream( F: TStream ); override;
    procedure WriteStream( F: TStream ); override;
  end;

  TBSONDatetimeItem = class( TBSONItem )
    FData: TDatetime;
  public
    constructor Create( AValue: TDateTime );

    procedure ReadStream( F: TStream ); override;
    procedure WriteStream( F: TStream ); override;
  end;

implementation

uses
  DateUtils;

const
  nullterm          : char = #0;

var
  buf               : array[0..1023] of char;
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
      BSON_FLOAT: FItems[high( FItems )] := TBSONDoubleItem.Create;
      BSON_INT32: FItems[high( FItems )] := TBSONIntItem.Create;
      BSON_INT64: FItems[high( FItems )] := TBSONInt64Item.Create;
      BSON_BOOLEAN: FItems[high( FItems )] := TBSONBooleanItem.Create;
      BSON_STRING: FItems[high( FItems )] := TBSONStringItem.Create;
      BSON_DOC: FItems[high( FItems )] := TBSONDocumentItem.Create;
      BSON_JS: FItems[high( FItems )] := TBSONJSItem.Create;
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
  f := TFileStream.Create( FileCreate( filename ) );
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
  startpos, lastpos : int64;
  dummy             : integer;
  i                 : integer;
begin
  startpos := F.Position;
  f.write( dummy, sizeof( dummy ) );
  for i := 0 to high( FItems ) do begin
    FItems[i].WriteStream( f );
  end;
  f.Write( nullterm, sizeof( nullterm ) );
  lastpos := F.Position;
  dummy := lastpos - startpos;
  f.Seek( startpos, soFromBeginning );
  f.Write( dummy, sizeof( dummy ) );
  f.Seek( lastpos, soFromBeginning );
end;

{ TBSONDoubleItem }

constructor TBSONDoubleItem.Create( AValue: real );
begin
  eltype := BSON_FLOAT;
  FData := AValue;
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

constructor TBSONItem.Create;
begin
  fnull := true;
  eltype := BSON_NULL;
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
  f.Write( eltype, sizeof( byte ) );
  f.Write( elname[1], length( elname ) );
  f.Write( nullterm, sizeof( nullterm ) );
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

procedure TBSONDocumentItem.ReadStream( F: TStream );
begin
  inherited;
  FData.ReadStream( f );
end;

procedure TBSONDocumentItem.WriteStream( F: TStream );
begin
  inherited;
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

function TBSONArrayItem.ReadItem( idx: integer ): TBSONItem;
begin
  Result := FData.Items[idx];
end;

procedure TBSONArrayItem.ReadStream( F: TStream );
begin
  inherited;
  FData.ReadStream( F );
end;

procedure TBSONArrayItem.WriteItem( idx: integer; item: TBSONItem );
begin
  inherited;
  FData.SetValue( IntToStr( idx ), item );
end;

procedure TBSONArrayItem.WriteStream( F: TStream );
begin
  inherited;
  FData.WriteStream( f );
end;

{ TBSONDatetimeItem }

constructor TBSONDatetimeItem.Create( AValue: TDateTime );
begin
  eltype := BSON_DATETIME;
  FData := AValue;
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

initialization
  nullitem := TBSONItem.Create;
finalization
  nullitem.Free;
end.

